
from pyspark import SparkConf, SparkContext
from pyspark.sql import *
import argparse
import os
import re
import shutil
import datetime

from pyspark.sql.types import StructField, StringType, StructType, IntegerType
from pyspark.sql.functions import udf
from pyspark.sql import functions as F


def get_parser():
    """
    Creates a parser to efficiently parse command line arguments
    """
    parser = argparse.ArgumentParser(description='Spark Dataframes')
    parser.add_argument("-i", "--input",
                        dest="inputdir",
                        type=str,
                        help="Input directory",
                        required=True)
    parser.add_argument("-o", "--output",
                        dest="outputdir",
                        type=str,
                        help="Output directory",
                        required=True)
    return parser


def processMSC():
    """
    Parses MSC records as per defined rules
    :return: Records returned in pipe-delimited format
    """
    # Assumption: MSC folder under the provided input path
    inputDir = os.path.join(args.inputdir, "INPUT")
    lines = sc.textFile(inputDir)

    # Call the parsing function
    parsedMSCLines = lines.map(parseMSCRecords)

    # The schema is encoded in a string.
    schemaString = "RecordType FirstNum SecondNum CallDate CallHour Duration StartTower StartLAC CallType"
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    # Apply the schema to the RDD.
    schemaData = sqlContext.createDataFrame(parsedMSCLines, schema)
    
    modify_phone_number_udf = udf(mod_number, StringType())
    ph_num_mod = schemaData.select(
        schemaData.RecordType,
        modify_phone_number_udf(schemaData.FirstNum).alias('FirstNum'),
        modify_phone_number_udf(schemaData.SecondNum).alias('SecondNum'),
        schemaData.CallDate,
        schemaData.CallHour,
        schemaData.Duration,
        schemaData.StartTower,
        schemaData.StartLAC,
        schemaData.CallType)

    get_phone_type_udf = udf(get_phone_type, StringType())

    first_ph_type = ph_num_mod.withColumn('FirstPhoneType', get_phone_type_udf(ph_num_mod.FirstNum))

    sec_ph_type = first_ph_type.withColumn('SecondPhoneType', get_phone_type_udf(first_ph_type.SecondNum))

    final_df = sec_ph_type.select(
        sec_ph_type.RecordType,
        sec_ph_type.FirstNum,
        sec_ph_type.SecondNum,
        sec_ph_type.CallDate,
        sec_ph_type.CallHour,
        sec_ph_type.Duration,
        sec_ph_type.StartTower,
        sec_ph_type.StartLAC,
        sec_ph_type.CallType,
        F.when(sec_ph_type.FirstPhoneType.isin(["mobile", "landline", "shortcode"])
               & sec_ph_type.SecondPhoneType.isin(["mobile", "landline", "shortcode"]), "National")
            .otherwise("International").alias('PhoneType'))

    print final_df.show()
    # converts the data to pipe delimited format
    #pipedMSCResults = parsedMSCLines.map(toPipeLine)

    # Save the results in a MSC folder under the output directory specified
    #outputdirMSC = os.path.join(args.outputdir, "MSC")
    #pipedMSCResults.saveAsTextFile(outputdirMSC)

def mod_number(input_number):
    # Variables

    global BLANK_PHONE_NUMBERS, STARTPTSUCOUNT, STARTW233COUNT, STARTSW791000COUNT, STARTB0COUNT, NATIONAL_COUNT\
        , INTERNATIONAL_COUNT, SHORTCODE_COUNT, TOTAL_NUMBER_COUNT, country_counts

    # Check for blank numbers and increment respective counters
    if len(input_number) == 0:
        BLANK_PHONE_NUMBERS += 1
    else:
        TOTAL_NUMBER_COUNT += 1

    # Remove 1A/1a from the end. eg: '1A'12345678 => 12345678
    if len(input_number) > 1 and (re.match("^1A[0-9]*$", input_number) or re.match("^1a[0-9]*$", input_number)):
        input_number = input_number[2:]

    # Remove F/f from the end. eg: 12345678'F' => 12345678
    if len(input_number) > 1 and re.match("^[0-9]*[a-zA-Z]$", input_number):
        input_number = input_number[:-1]

    # Remove FF/ff from the end or any alphabet eg: 12345678'FF' => 12345678
    if len(input_number) > 1 and re.match("^[0-9]*[a-zA-Z][a-zA-Z]$", input_number):
        input_number = input_number[:-2]

    # Remove alphabets which are last but one. eg: 12345678'F8' => 12345678
    if len(input_number) > 1 and re.match("^[0-9]*[A-Z][0-9]$", input_number):
        input_number = input_number[:-2]

    # Numbers starting with PTSU
    if input_number[0:4] == 'PTSU':
        input_number = input_number[4:]
        STARTPTSUCOUNT += 1

    # Numbers starting with 2330
    if input_number[0:4] == "2330":
        input_number = input_number[4:]
        STARTW233COUNT += 1

    # Numbers starting with 791000
    if input_number[0:6] == "791000":
        input_number = input_number[6:]
        STARTSW791000COUNT += 1

    # Numbers starting with 'B0'
    if len(input_number) > 1 and input_number[0:2] == "B0":
        input_number = input_number[2:]
        STARTB0COUNT += 1

    # Remove any leading zeroes
    input_number = input_number.lstrip('0')

    # Strip the Afghanistan numbers to 9 digits (Afghanistan Code is 93)
    if len(input_number) == 11 and input_number[0:2] == "93":
        input_number = input_number[2:]

    return input_number


def get_phone_type(input_number):
    global NATIONAL_COUNT, INTERNATIONAL_COUNT, SHORTCODE_COUNT

    if len(input_number) == 11 and input_number[0:2] in new_landline_codes:
        numberType = "landline"
        NATIONAL_COUNT += 1
    elif len(input_number) == 9 or len(input_number) == 10:
        numberType = "landline"
        NATIONAL_COUNT += 1
    elif len(input_number) == 9 and input_number[0] == '7':
        numberType = "mobile"
        NATIONAL_COUNT += 1
    elif len(input_number) < 9:
        numberType = "shortcode"
        SHORTCODE_COUNT += 1
    else:
        # Eliminate the numbers that have an alphabet in the middle
        if (re.match("^[0-9]*[A-Z][0-9]*$", input_number)) is not None:
            BLANK_PHONE_NUMBERS += 1
        else:
            for eachKey in intl_codes.keys():
                if input_number.startswith(eachKey):
                    numberType = "intl"
                    INTERNATIONAL_COUNT += 1

    return numberType


def parseMSCRecords(recordMSC):
    """
    Parse each MSC record as per defined rules
    :param recordMSC: Each record/line in the file
    :return: Returns extracted fields for each record
    """

    # Split the semi colon separated record
    fields = recordMSC.split(';')

    # Extract required fields - 0,2,3,8,10,13,14,15
    RecordType = fields[0]
    FirstNum = fields[2]
    SecondNum = fields[3]
    StartTower = fields[8]
    StartLAC = fields[10]
    CallDate = fields[13]
    CallHour = fields[14]
    Duration = fields[15]

    # Define the Call type using the Record Type
    if RecordType in ('0', '1'):
        CallType = "CALL"
    elif RecordType in ('6', '7'):
        CallType = "SMS"
    elif RecordType in '9':
        CallType = "Emergency Call"
    else:
        CallType = "Other"

    return RecordType, FirstNum, SecondNum, CallDate, CallHour, Duration, StartTower, StartLAC, CallType



def hms_string(sec_elapsed):
    """
    Returns the time in a formatted manner
    :param sec_elapsed: Time in seconds
    :return: Returns time in HH:MM:SS:ss
    """
    h = int(sec_elapsed / (60 * 60))
    m = int((sec_elapsed % (60 * 60)) / 60)
    s = sec_elapsed % 60.
    return "{}:{:>02}:{:>05.2f}".format(h, m, s)


####################################################
# Starting point of the code
# Configure Spark
####################################################
if __name__ == "__main__":

    # Configure Spark
    conf = SparkConf().setMaster("local").setAppName("Parsing Script")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # Execute Main functionality
    # Log the start time of the code
    start_time = datetime.datetime.now()
    print '\n'
    print '-' * 50
    print "Start Time:", start_time

    # Parse sys.argv using parser
    print "Processing input arguments.."
    args = get_parser().parse_args()
    print "Input Staging Directory:", args.inputdir
    print "Output Staging Directory:", args.outputdir

    # Remove output directory if exists
    if os.path.exists(args.outputdir):
        shutil.rmtree(args.outputdir)

    # Define Accumulator variables
    NO_INPUT_RECORDS = sc.accumulator(0)
    BLANK_PHONE_NUMBERS = sc.accumulator(0)
    STARTPTSUCOUNT = sc.accumulator(0)
    STARTW233COUNT = sc.accumulator(0)
    STARTSW791000COUNT = sc.accumulator(0)
    STARTB0COUNT = sc.accumulator(0)
    NATIONAL_COUNT = sc.accumulator(0)
    INTERNATIONAL_COUNT = sc.accumulator(0)
    SHORTCODE_COUNT = sc.accumulator(0)
    TOTAL_NUMBER_COUNT = sc.accumulator(0)

    # Process MSC
    print '-' * 50
    print "Begin processing MSC files.."
    processMSC()
    print '-' * 50

    # Log the counts
    print "NUMBER OF MSC RECORDS:", NO_INPUT_RECORDS.value
    print "INVALID/BLANK PHONE NUMBERS:", BLANK_PHONE_NUMBERS.value
    print "TOTAL NUMBER OF PHONE NUMBER OCCURRENCES:", TOTAL_NUMBER_COUNT.value
    print "NUMBERS STARTING WITH PTSU:", STARTPTSUCOUNT.value
    print "NUMBERS STARTING WITH 2330:", STARTW233COUNT.value
    print "NUMBERS STARTING WITH 791000:", STARTSW791000COUNT.value
    print "NUMBERS STARTING WITH B0:", STARTB0COUNT.value
    print "NUMBER OF NATIONAL NUMBERS:", NATIONAL_COUNT.value
    print "NUMBER OF INTERNATIONAL NUMBERS:", INTERNATIONAL_COUNT.value
    print "NUMBER OF SHORTCODE NUMBERS:", SHORTCODE_COUNT.value
    print '-' * 50

    # Log the end time of the code
    end_time = datetime.datetime.now()
    print "End Time:", end_time

    # Log the total proceesing time taken
    seconds_elapsed = (end_time - start_time).total_seconds()
    print "\nTOTAL TIME TAKEN: {}".format(hms_string(seconds_elapsed))

    print "FILE PROCESSING COMPLETED SUCCESSFULLY !"

    ####################################################
    # End of Code
    ####################################################
