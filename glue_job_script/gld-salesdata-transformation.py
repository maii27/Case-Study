import sys
import re
import uuid
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name, split, lit, concat, col, coalesce,regexp_extract,current_timestamp, to_timestamp, when, udf, substring,regexp_extract
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

def transformed(glue_context,spark,df_transformed):
    #Get max id
    # max_id=0
    max_id_query= '''
    SELECT
        coalesce(max(id),0) as max_id
    FROM glue_catalog.sil.sales_transaction
    '''
    df=spark.sql(max_id_query)
    max_id=df.collect()[0]['max_id']
    
    df_transformed = df_transformed.withColumn("id", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())).cast(LongType()))
    df_transformed = df_transformed.withColumn("id",df_transformed["id"]+max_id)
    df_transformed = df_transformed.na.drop(subset=[
                                            "order_id", "product", "quantity_ordered",
                                            "price_each", "order_date", "purchase_address"
                                        ])
    
    #Extract city and code from Purchase Address
    get_city = udf(lambda address: address.split(", ")[1], StringType())
    df_transformed = df_transformed.withColumn("city", get_city(df_transformed["purchase_address"]))
    df_transformed = df_transformed.withColumn("code", substring(df_transformed["purchase_address"], -5, 5))
    df_transformed = df_transformed.withColumn("state", regexp_extract("purchase_address", r",\s*([A-Z]{2})\s*\d{5}", 1))
    
    df_transformed = df_transformed.withColumn("update_timestamp", current_timestamp())
    #Change column order
    columns_to_move = ["id"]
    df_transformed = df_transformed.select(columns_to_move + [col for col in df_transformed.columns if col not in columns_to_move])
    dyf= DynamicFrame.fromDF(df_transformed, glue_context,name="df_final")

    return dyf

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
        dyf = read_table(glue_context, source_db_name="brnz", table_name= 'sales_data')

        df_transformed = dyf.toDF()
        dyf_final = transformed(glue_context,spark_session,df_transformed)
        write_table(glue_context, target_db_name="gld", table_dyf=dyf_final, table_name="sales_transaction", s3_base_url=s3_base_url)
        
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