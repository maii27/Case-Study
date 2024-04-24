CREATE EXTERNAL TABLE `dim_product_category`(
  `id` bigint, 
  `product_category` string, 
  `update_timestamp` timestamp)
LOCATION
  's3://dev-datalake/curated_test/gld/dim_product_category/'
TBLPROPERTIES (
  'CreatedByJob'='bookdata', 
  'CreatedByJobRun'='jr_ee8f61a75708e1cc194045312d66657cd9a0c6d0ac19f5cc61e5edce45782de5', 
  'classification'='parquet', 
  'useGlueParquetWriter'='true')
 