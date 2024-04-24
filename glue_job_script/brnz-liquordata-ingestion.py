import sys
import re
import boto3
import uuid
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name, split, lit, concat, col, coalesce, expr ,year, month, dayofmonth,regexp_extract,current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType,LongType
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
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

def read_csv(glue_context, csv_input_path, previous_latest_timestamp):
    s3_client = boto3.client('s3')
    bucket_name = 'ampd-aldous-dev-datalake'
    prefix = 'landing_study/liquor_data/'
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    latest_timestamp = None
    if 'Contents' in response:
        latest_timestamp = max(obj['LastModified'] for obj in response['Contents'])

    if previous_latest_timestamp is None:
        # Ingest new data for the first run
        if latest_timestamp:
            new_files = [obj['Key'] for obj in response['Contents'] if obj['LastModified'] == latest_timestamp]
            csv_input_path = [f"s3://{bucket_name}/{file}" for file in new_files]
        text_df = glue_context.spark_session.read \
                                            .option("encoding", "UTF-8") \
                                            .option("delimiter", ",") \
                                            .option("header", "true") \
                                            .csv(csv_input_path)
        text_df = text_df.withColumn("file_name", input_file_name())
        dyf = DynamicFrame.fromDF(text_df, glue_context, "dyf")
        return dyf, latest_timestamp
    elif latest_timestamp and latest_timestamp > previous_latest_timestamp:
        # Ingest data only from the file with the maximum timestamp after the first runf
        new_files = [obj['Key'] for obj in response['Contents'] if obj['LastModified'] == latest_timestamp]
        csv_input_path = [f"s3://{bucket_name}/{file}" for file in new_files]
        text_df = glue_context.spark_session.read \
                                            .option("encoding", "UTF-8") \
                                            .option("delimiter", ",") \
                                            .option("header", "true") \
                                            .csv(csv_input_path)
        text_df = text_df.withColumn("file_name", input_file_name())
        dyf = DynamicFrame.fromDF(text_df, glue_context, "dyf")
        return dyf, latest_timestamp
    
def write_table(glue_context, target_db_name, table_dyf, table_name, s3_base_url):
    s3output = glue_context.getSink(
    path=f"{s3_base_url}/{target_db_name}/{table_name}",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=['year', 'month', 'day'],
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
    df_transformed = df_transformed.withColumnRenamed("invoice/item number", "invoice_number") \
                                    .withColumnRenamed("store number", "store_number") \
                                    .withColumnRenamed("store name", "store_name") \
                                    .withColumnRenamed("address", "address") \
                                    .withColumnRenamed("city", "city") \
                                    .withColumnRenamed("zip code", "zip_code") \
                                    .withColumnRenamed("store location", "store_location") \
                                    .withColumnRenamed("county number", "county_number") \
                                    .withColumnRenamed("county", "county") \
                                    .withColumnRenamed("category", "category") \
                                    .withColumnRenamed("category name", "category_name") \
                                    .withColumnRenamed("vendor number", "vendor_number") \
                                    .withColumnRenamed("vendor name", "vendor_name") \
                                    .withColumnRenamed("item number", "item_number") \
                                    .withColumnRenamed("item description", "item_description") \
                                    .withColumnRenamed("pack", "pack") \
                                    .withColumnRenamed("bottle volume (ml)", "bottle_volume_ml") \
                                    .withColumnRenamed("state bottle cost", "state_bottle_cost") \
                                    .withColumnRenamed("state bottle retail", "state_bottle_retail") \
                                    .withColumnRenamed("bottles sold", "bottles_sold") \
                                    .withColumnRenamed("sale (dollars)", "sales_amount") \
                                    .withColumnRenamed("volume sold (liters)", "volume_sold_litres") \
                                    .withColumnRenamed("volume sold (gallons)", "volume_sold_gallons")
    
    date_pattern = r'_(\d{8})'
    df_transformed = df_transformed.withColumn("input_file", input_file_name())
    df_transformed = df_transformed.withColumn('file_date', regexp_extract(input_file_name(), date_pattern, 1))
    df_transformed = df_transformed.withColumn("file_date", expr("TO_DATE(CAST(file_date AS STRING), 'yyyyMMdd')"))
    df_transformed = df_transformed.withColumn("product_category", lit("liquor"))
    
    
    #Extract to get partition
    df_transformed = df_transformed.withColumn("year", year("file_date"))
    df_transformed = df_transformed.withColumn("month", month("file_date"))
    df_transformed = df_transformed.withColumn("day", dayofmonth("file_date"))
    
    # Change data type
    df_transformed = df_transformed.withColumn("year", F.trim(df_transformed["year"]))
    df_transformed = df_transformed.withColumn("date", to_date(col("date"), "MM/dd/yyyy"))
    df_transformed = df_transformed.withColumn("store_number", col("store_number").cast("bigint"))
    df_transformed = df_transformed.withColumn("county_number", col("county_number").cast("bigint"))
    df_transformed = df_transformed.withColumn("vendor_number", col("vendor_number").cast("bigint"))
    df_transformed = df_transformed.withColumn("pack", col("pack").cast("int"))
    df_transformed = df_transformed.withColumn("bottle_volume_ml", col("bottle_volume_ml").cast("int"))
    df_transformed = df_transformed.withColumn("bottles_sold", col("bottles_sold").cast("int"))
    df_transformed = df_transformed.withColumn("state_bottle_cost", col("state_bottle_cost").cast("float"))
    df_transformed = df_transformed.withColumn("state_bottle_retail", col("state_bottle_retail").cast("float"))
    df_transformed = df_transformed.withColumn("sales_amount", col("sales_amount").cast("float"))
    df_transformed = df_transformed.withColumn("volume_sold_litres", col("volume_sold_litres").cast("float"))
    df_transformed = df_transformed.withColumn("volume_sold_gallons", col("volume_sold_gallons").cast("float"))
    df_transformed = df_transformed.withColumn("year", col("year").cast("int"))
    df_transformed = df_transformed.withColumn("month", col("month").cast("int"))
    df_transformed = df_transformed.withColumn("day", col("day").cast("int"))
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
    csv_input_path = "s3://ampd-aldous-dev-datalake/landing_study/liquor_data/"
    job, glue_context, spark_session = init_job(_job_args)
    
    try: 
        spark = glue_context.spark_session
        previous_latest_timestamp = None  
        dyf, latest_timestamp = read_csv(glue_context, csv_input_path, previous_latest_timestamp)
    
        if dyf:
            df_transformed = dyf.toDF()
            dyf_final = transformed(glue_context, df_transformed)
        previous_latest_timestamp = latest_timestamp
        truncate_table(glue_context,spark_session,target_db_name="brnz",table_name="liquor_data")
        write_table(glue_context, target_db_name="brnz", table_dyf=dyf_final, table_name="liquor_data", s3_base_url=s3_base_url)
        
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