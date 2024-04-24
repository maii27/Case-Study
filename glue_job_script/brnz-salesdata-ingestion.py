import sys
import re
import boto3
import uuid
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name, split, lit, concat, col, coalesce, expr ,year, month, dayofmonth,regexp_extract,current_timestamp, to_timestamp, when
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

def init_job(job_args):
    sc = SparkContext()
    _glue_context = GlueContext(sc)
    _spark_session = _glue_context.spark_session
    _job = Job(_glue_context)
    _job.init(job_args['job_name'], job_args)
    return _job, _glue_context, _spark_session

def read_csv(glue_context, csv_input_path,processed_files):
    s3_client = boto3.client('s3')
    bucket_name = 'ampd-aldous-dev-datalake' 
    prefix = 'landing_study/sales_data/'
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    current_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].strip() and obj['Key'][-1] != '/'] 
    new_files = [file for file in current_files if file not in processed_files] 

    if new_files:
        new_file_names = [file.split('/')[-1] for file in new_files]
        new_files_to_process = [file for file in new_file_names if file not in processed_files]
        
        if new_files_to_process:
            csv_input_paths = [f"s3://{bucket_name}/{prefix}{file}" for file in new_files_to_process]
            text_df = glue_context.spark_session.read \
                                                .option("encoding", "UTF-8") \
                                                .option("delimiter", ",") \
                                                .option("header", "true") \
                                                .csv(csv_input_paths)
            text_df = text_df.withColumn("file_name", input_file_name())
            dyf = DynamicFrame.fromDF(text_df, glue_context, "dyf")
            return dyf, new_files_to_process
        else:
            return None, []
    else:
        return None, []
        
def write_table(glue_context, target_db_name, table_dyf, table_name, s3_base_url):
    s3output = glue_context.getSink(
    path=f"{s3_base_url}/{target_db_name}/{table_name}",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=['year', 'month'],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output",
        )
    s3output.setCatalogInfo(
        catalogDatabase=target_db_name, catalogTableName=table_name
        )
    s3output.setFormat("glueparquet")
    s3output.writeFrame(table_dyf)

def transformed(glue_context,df_transformed):
    df_transformed = df_transformed.withColumnRenamed("order id", "order_id") \
                                    .withColumnRenamed("quantity ordered", "quantity_ordered") \
                                    .withColumnRenamed("price each", "price_each") \
                                    .withColumnRenamed("order date", "order_date") \
                                    .withColumnRenamed("purchase address", "purchase_address") \

    df_transformed = df_transformed.withColumn("file_name", regexp_extract(col("file_name"), "[^/]*$", 0)) \
           .withColumn("year", regexp_extract(col("file_name"), "(\d{4})\.csv$", 1)) \
           .withColumn("month_name", split(col("file_name"), "_")[1]) \
           .withColumn("month", 
                       when(col("month_name") == "January", 1)
                       .when(col("month_name") == "February", 2)
                       .when(col("month_name") == "March", 3)
                       .when(col("month_name") == "April", 4)
                       .when(col("month_name") == "May", 5)
                       .when(col("month_name") == "June", 6)
                       .when(col("month_name") == "July", 7)
                       .when(col("month_name") == "August", 8)
                       .when(col("month_name") == "September", 9)
                       .when(col("month_name") == "October", 10)
                       .when(col("month_name") == "November", 11)
                       .when(col("month_name") == "December", 12)
                       .otherwise(None))
                       
    date_pattern = r'_(\d{8})'
    df_transformed = df_transformed.withColumn("input_file", input_file_name())
    df_transformed = df_transformed.withColumn("product_category", lit("electronics"))
    
    # Change data type
    df_transformed = df_transformed.withColumn("year", F.trim(df_transformed["year"]))
    df_transformed = df_transformed.withColumn("order_date", to_timestamp(col("order_date"), "M/d/yyyy H:mm"))
    df_transformed = df_transformed.withColumn("order_id", col("order_id").cast("bigint"))
    df_transformed = df_transformed.withColumn("quantity_ordered", col("quantity_ordered").cast("int"))
    df_transformed = df_transformed.withColumn("price_each", col("price_each").cast("float"))

    df_transformed = df_transformed.withColumn("year", col("year").cast("int"))
    df_transformed = df_transformed.withColumn("month", col("month").cast("int"))

    df_transformed = df_transformed.withColumn("update_timestamp", current_timestamp())
    
    dyf= DynamicFrame.fromDF(df_transformed, glue_context,name="df_final")

    return dyf

def truncate_table(glue_context,spark,target_db_name,table_name):
    if spark.catalog.tableExists(table_name,target_db_name):
        glue_context.purge_table(target_db_name, table_name, {"retentionPeriod": 0})
        
def log_job_status(status, timestamp, error_message=None):
    schema = StructType([
        StructField("log_id", StringType()),
        StructField("batch_id", StringType()),
        StructField("job_ctnl_id", StringType()),
        StructField("log_status", StringType()),
        StructField("process_date", StringType()),
        StructField("error_message", StringType()),
        StructField("created_ts", StringType())
    ])

    s3_bucket = 'ampd-aldous-dev-datalake'
    s3_key = 'curated_test/airflow/etl_log.parquet'

    spark = SparkSession.builder.appName("log_job_status").getOrCreate()
    data = [(str(uuid.uuid4()), "", "", status, timestamp.strftime("%Y-%m-%d"), error_message, datetime.now().isoformat())]
    df = spark.createDataFrame(data, schema)

    df.write.mode("append").parquet(f"s3://{s3_bucket}/{s3_key}")

    spark.stop()
    
def main():
    _job_args = getResolvedOptions(sys.argv, ["job_name"
                                              ,"datalake_bucket"
                                              ])

    datalake_bucket = _job_args['datalake_bucket']
    s3_base_url=f"s3://{datalake_bucket}/curated_test"
    csv_input_path = "s3://ampd-aldous-dev-datalake/landing_study/sales_data/"
    job, glue_context, spark_session = init_job(_job_args)
    try:
        spark = glue_context.spark_session
        processed_files = []  
        dyf, new_files = read_csv(glue_context, csv_input_path, processed_files)
    
        if dyf:
            df_transformed = dyf.toDF()
            dyf_final = transformed(glue_context, df_transformed)
    
        truncate_table(glue_context,spark_session,target_db_name="brnz",table_name="sales_data")
        write_table(glue_context, target_db_name="brnz", table_dyf=dyf_final, table_name="sales_data", s3_base_url=s3_base_url)
          
          # Log success
        log_job_status(status="SUCCESS", timestamp=datetime.now())
        print("Job completed successfully.")
    except Exception as e:
        # Log failure
        log_job_status(status="FAIL", timestamp=datetime.now(), error_message=str(e))
        print(f"Job failed with error: {str(e)}")   
        
    job.commit()


if __name__ == '__main__':
    main()