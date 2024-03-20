import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date
from datetime import datetime

# define var
JOB_NAME = 'richie_glue'
INPUT_BUCKET = 'richie-pratice-data-lake'
INPUT_PATH = f"s3://{INPUT_BUCKET}"
OUTPUT_BUCKET = 'richie-glue-output'
OUTPUT_PATH = f"s3://{OUTPUT_BUCKET}/output.parquet"
DATABASE_NAME = 'ricie-glue-job'
TABLE_NAME = 'orders'
PARTITION_KEY = 'item_system__c'

# 取得命令列參數
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Init Spark, Glu, job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)



# deine schema 
schema = StructType([
    StructField("Id", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("CreatedDate", StringType(), True),
    StructField("date_id__c", StringType(), True),
    StructField("sale_num__c", IntegerType(), True),
    StructField("sale_seq__c", IntegerType(), True),
    StructField("item_id__c", StringType(), True),
    StructField("item_code__c", StringType(), True),
    StructField("item_system__c", StringType(), True),
    StructField("qty__c", IntegerType(), True),
    StructField("Amount", IntegerType(), True),
    StructField("Brand", StringType(), True),
    StructField("ExternalId__c", StringType(), True),
    StructField("md_account__c", StringType(), True),
    StructField("sale_status__c", StringType(), True),
    StructField("channel_sale_num__c", StringType(), True),
    StructField("no__c", StringType(), True),
])

# Create glue data catalog
dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name=TABLE_NAME,
    transformation_ctx="datasource0",
    path=INPUT_PATH,
    format="csv",
    additional_options={"header": True}
)

# Convert date_id__c to date format
dyf_df = dyf.toDF()
dyf_df = dyf_df.withColumn("date_id__c", to_date(col("date_id__c"), "yyyy/MM/dd"))
dyf_dyf = glueContext.create_dynamic_frame.fromDF(dyf_df, glueContext, "dyf")

# # Write to parquet format
# glueContext.write_dynamic_frame.from_options(
#     frame=dyf_dyf,
#     connection_type="s3",
#     connection_options={"path": OUTPUT_PATH},
#     format="parquet",
#     transformation_ctx="datasink"
# )


job.commit()
