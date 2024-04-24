import sys
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name, split, lit, concat, col, coalesce, expr ,year, month, dayofmonth,regexp_extract,current_timestamp, regexp_replace
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta


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
    df_transformed = df_transformed.withColumnRenamed("Title", "title") \
                                    .withColumnRenamed("description", "description") \
                                    .withColumnRenamed("authors", "authors") \
                                    .withColumnRenamed("image", "image") \
                                    .withColumnRenamed("previewLink", "preview_link") \
                                    .withColumnRenamed("publisher", "publisher") \
                                    .withColumnRenamed("publishedDate", "published_date") \
                                    .withColumnRenamed("infoLink", "info_link") \
                                    .withColumnRenamed("categories", "categories") \
                                    .withColumnRenamed("ratingsCount", "ratings_count")
    
    df_transformed = df_transformed.withColumn("authors", regexp_replace("authors", r"[\[\]']", "")) \
                                    .withColumn("categories", regexp_replace("categories", r"[\[\]']", "")) \
    
    date_pattern = r'_(\d{8})'
    df_transformed = df_transformed.withColumn("input_file", input_file_name())
    df_transformed = df_transformed.withColumn('file_date', regexp_extract(input_file_name(), date_pattern, 1))
    df_transformed = df_transformed.withColumn("file_date", expr("TO_DATE(CAST(file_date AS STRING), 'yyyyMMdd')"))
    #Extract to get partition
    df_transformed = df_transformed.withColumn("year", year("file_date"))
    df_transformed = df_transformed.withColumn("month", month("file_date"))
    df_transformed = df_transformed.withColumn("day", dayofmonth("file_date"))
    # Change data type
    df_transformed = df_transformed.withColumn("year", F.trim(df_transformed["year"]))
    df_transformed = df_transformed.withColumn("ratings_count", col("ratings_count").cast("int"))
    df_transformed = df_transformed.withColumn("year", col("year").cast("int"))
    df_transformed = df_transformed.withColumn("month", col("month").cast("int"))
    df_transformed = df_transformed.withColumn("day", col("day").cast("int"))
    df_transformed = df_transformed.withColumn("update_timestamp", current_timestamp())
    
    dyf= DynamicFrame.fromDF(df_transformed, glue_context,name="df_final")

    return dyf

def truncate_table(glue_context,spark,target_db_name,table_name):
    if spark.catalog.tableExists(table_name,target_db_name):
        glue_context.purge_table(target_db_name, table_name, {"retentionPeriod": 0})

def main():
    _job_args = getResolvedOptions(sys.argv, ["job_name"
                                              ,"datalake_bucket"
                                              ])

    datalake_bucket = _job_args['datalake_bucket']
    s3_base_url=f"s3://{datalake_bucket}/curated_test"
    job, glue_context, spark_session = init_job(_job_args)
    dyf = read_table(glue_context, source_db_name="sri", table_name= 'book_data')
    num_records=dyf.count()

    if num_records > 0:
        df_transformed = dyf.toDF()
        dyf_final = transformed(glue_context,df_transformed)
        truncate_table(glue_context,spark_session,target_db_name="brnz",table_name="book_data")
        write_table(glue_context, target_db_name="brnz", table_dyf=dyf_final, table_name="book_data", s3_base_url=s3_base_url)
        
    job.commit()


if __name__ == '__main__':
    main()
