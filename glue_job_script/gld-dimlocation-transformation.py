import sys
import re
import uuid
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name, split, lit, col, coalesce,regexp_extract,current_timestamp, to_timestamp, when,upper, first
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

def read_table(glue_context, source_db_name, table_name):
    _tbl_dyf = glue_context.create_dynamic_frame.from_catalog(database=source_db_name, table_name=table_name, transformation_ctx="_tbl_dyf")
    return _tbl_dyf
    
def write_table(glue_context, target_db_name, table_dyf, table_name, s3_base_url):
    s3output = glue_context.getSink(
    path=f"{s3_base_url}/{target_db_name}/{table_name}",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output",
        )
    s3output.setCatalogInfo(
        catalogDatabase=target_db_name, catalogTableName=table_name
        )
    s3output.setFormat("glueparquet")
    s3output.writeFrame(table_dyf)

def transformed(glue_context,df_sales_data,df_liquor_data):
    df_sales_data = df_sales_data.withColumnRenamed("code", "zipcode")
    df_sales_data = df_sales_data.select(upper("zipcode").alias("zip_code"), 
                                     upper("city").alias("city"), 
                                     upper("state").alias("state")).distinct()

    df_liquor_data = df_liquor_data.select(upper("zip_code").alias("zip_code"), upper("city").alias("city")).distinct()
    df_liquor_data = df_liquor_data.withColumn("state", lit(None))
    df_transformed = df_sales_data.union(df_liquor_data)
    df_transformed = df_transformed.groupby("zip_code").agg(first("city").alias("city_name"), first("state").alias("state")).dropDuplicates()
    df_transformed = df_transformed.dropna(subset=["zip_code"])
    
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
    job, glue_context, spark_session = init_job(_job_args)
    try:
        sales_data = read_table(glue_context, source_db_name="gld", table_name= 'sales_transaction')
        liquor_data = read_table(glue_context, source_db_name="gld", table_name= 'liquor_transaction')
    
        df_sales_data = sales_data.toDF()
        df_liquor_data = liquor_data.toDF()
        dyf_final = transformed(glue_context,df_sales_data,df_liquor_data)
        write_table(glue_context, target_db_name="gld", table_dyf=dyf_final, table_name="dim_location", s3_base_url=s3_base_url)
        
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