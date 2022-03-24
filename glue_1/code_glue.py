###########################
#--The libraries imports--#
###########################

import sys
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *#col, concat_ws, lit
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import *
from datetime import datetime
import time
import logging
import boto3
from awsglue.transforms import *
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col, collect_list, max

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import collect_list, sort_array
from pyspark.sql.functions import udf,sort_array,collect_list
from pyspark.sql.types import StringType

from pyspark.sql.functions import explode, collect_list
from pyspark.sql.functions import monotonically_increasing_id, concat,lit
from pyspark.sql.functions import trim, regexp_replace
from pyspark.sql.functions import split, regexp_replace, cast, upper
from pyspark.sql.functions import concat, substring, to_date, when
from pyspark.sql.functions import count
from pyspark.sql.functions import monotonically_increasing_id, concat,lit
import hashlib


########################
#--Functions Sections--#
########################

def clear_s3_path(bucket_name: str, prefix: str) -> None:
    """
    Removes all the Objects from a S3 Bucket that share a common Prefix

        Parameters:
            bucket_name (str): Name of the buckets where the objects to delete are located
            prefix (str): Common prefix of the S3 Objects to delete

        Returns:
            None
    """

    s3 = boto3.resource('s3')

    bucket = s3.Bucket(bucket_name)
    bucket.objects.filter(Prefix=prefix).delete()




#----------------------------------------------------------------------------#
#                              NAME:hash_values                              #
#----------------------------------------------------------------------------#
# DESCRIPTION:                                                               #
#                 Esta es una funcion para hashear una lista en sha256       #
#----------------------------------------------------------------------------#
# ARGUMENTS:                                                                 #
#   values --  lista que contiene ordenadamente los valores                  #
#----------------------------------------------------------------------------#
# RETURN:                                                                    #
#   u -- the hash value than represents the unique list                      #
#----------------------------------by-iMarck---------------------------------#
def hash_values(values):
    return hashlib.sha256(",".join(values).encode()).hexdigest()
hash_udf = udf(hash_values, StringType())

###welcome
print("'####:'##::::'##::::'###::::'########:::'######::'##:::'##:")
print(". ##:: ###::'###:::'## ##::: ##.... ##:'##... ##: ##::'##::")
print(": ##:: ####'####::'##:. ##:: ##:::: ##: ##:::..:: ##:'##:::")
print(": ##:: ## ### ##:'##:::. ##: ########:: ##::::::: #####::::")
print(": ##:: ##. #: ##: #########: ##.. ##::: ##::::::: ##. ##:::")
print(": ##:: ##:.:: ##: ##.... ##: ##::. ##:: ##::: ##: ##:. ##::")
print("'####: ##:::: ##: ##:::: ##: ##:::. ##:. ######:: ##::. ##:")
print("....::..:::::..::..:::::..::..:::::..:::......:::..::::..::")
print("")


# Set logger to debug Mode
logger = logging.getLogger()
logger.setLevel(logging.INFO)
args = getResolvedOptions(sys.argv, [
                          'JOB_NAME', 'TABLE_DYNAMO','PROCESS'])


TABLE_DYNAMO = args['TABLE_DYNAMO']
PROCESS = args['PROCESS']
extension='.parquet'

inicio = time.time()

spark = SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer').config(
    'spark.sql.hive.convertMetastoreParquet', 'false').getOrCreate()
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



TABLE_DYNAMO = args['TABLE_DYNAMO']

dynamodb = boto3.resource('dynamodb')

client = boto3.client('dynamodb')
response = client.get_item(TableName=TABLE_DYNAMO, Key={
                           'process': {'S': str(PROCESS)}})

print(response)

#####################
#--DynamoDB Params--#
#####################

FULL_FORMAT = "*.parquet"
DAILY_FORMAT = "*.parquet"
INPUT_BUCKET = response['Item']['bucket_in']['S']
OUTPUT_BUCKET = response['Item']['bucket_out']['S']
IN_LOCATION = response['Item']['prefix_in']['S']
IN_LOCATION_CDC = response['Item']['prefix_in_cdc']['S']
OUT_LOCATION = response['Item']['prefix_out']['S']
TABLE_NAME = response['Item']['table']['S']
DATABASE = response['Item']['db']['S']

PK = "vin, saa"
PRECOMBINE = "year, month, day"

fin = time.time()
print("tiempo de lectura de parametros: ", fin - inicio)

prefix_read=f"{IN_LOCATION_CDC}"
print(f"prefix read {prefix_read}")

##################
#--Reading Data--#
##################

files='s3://{}/{}'.format(INPUT_BUCKET, prefix_read)

print(f'reading {files}')
inputDF = spark.read.parquet(f"{files}")

inputDF.printSchema()

inputDF=inputDF.repartition(100).coalesce(50)
inputDF=inputDF.select(col('vin'),col('series'),col('part_number')).distinct()

inputDF.printSchema()

##I select only those that have the series to filter and more than 2k pieces
inputDF_filtered = inputDF.filter((inputDF.series == 'MODEL1') | (inputDF.series == 'MODEL2'))
inputDF_filtered.show(10)

print('hashing...')
#to part_number pn

df_hash = inputDF_filtered.select(col('vin'),col('part_number')).distinct()\
    .groupBy('vin')\
    .agg(sort_array(collect_list('part_number')).alias('code_pn_v'))
    
df_hash = df_hash.withColumn("code_pn_hash", hash_udf(df_hash["code_pn_v"]))

##grouping by the hashing groups

df_hash.printSchema()

prelim_etec = df_hash.select(col('code_pn_hash')).distinct().withColumn("group_pn", concat(lit("group_"), (monotonically_increasing_id() + 1).cast("string")))

#prelim_etec.show(10)

df_hash=df_hash.join(prelim_etec, ["code_pn_hash"], "inner") \
    .select(prelim_etec.group_pn,df_hash.vin,df_hash.code_pn_hash, df_hash.code_pn_v).distinct()


df_hash.printSchema()

inicio2 = time.time()

clear_s3_path(bucket_name=OUTPUT_BUCKET, prefix=OUT_LOCATION)
path='s3://' + OUTPUT_BUCKET + '/' + OUT_LOCATION + ''

print(f'cleaned path {path}')

df_hash=df_hash.repartition(100).coalesce(50)
df_hash.write.mode('append').parquet(path)

print('Writed DF')

fin = time.time()
print("tiempo de escritura: ", fin - inicio2)
