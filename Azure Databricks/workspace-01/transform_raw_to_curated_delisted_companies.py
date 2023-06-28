# Databricks notebook source
## TEST PARAMS
# AS_OF_DT = 20230628
# STOFCK_SYMBOL = 0,1,2,3,4,5

# COMMAND ----------

## IMPORT LIBRARY
import requests
import json
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, expr, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, LongType

# COMMAND ----------

## NOTEBOOK PARAMS
dbutils.widgets.text("PAGE", "")
dbutils.widgets.text("AS_OF_DT", "")

## ASSIGN VARAIBLES
in_page = dbutils.widgets.get("PAGE")
in_page_list = in_page.split(',')
as_of_dt = dbutils.widgets.get("AS_OF_DT")
asql_table_name = 'curated.delisted_companies'

# COMMAND ----------

def check_path_from_raw(raw_filepath):
    try:
        dbutils.fs.ls(raw_filepath)
    except Exception as e:
        raise ValueError(f"file not found. Error:{e}")

# COMMAND ----------

def connect_jdbc():
    jdbcHostname = "temp-db-server.database.windows.net"
    jdbcDatabase = "temp-db"
    jdbcPort = 1433
    jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
    connectionProperties = {
        "user" : dbutils.secrets.get(scope="keyvault_scope_01",key="asqlusername"),
        "password" : dbutils.secrets.get(scope="keyvault_scope_01",key="asqlpassword"),
        "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    return jdbcUrl,connectionProperties

# COMMAND ----------

def query_asql(table_name):
    jdbcUrl,connectionProperties = connect_jdbc()
    query_df = spark.read.jdbc(url=jdbcUrl, table=table_name, properties=connectionProperties)
    return(query_df)

# COMMAND ----------

def write_on_asql_overwrite(writing_df,table_name):
    jdbcUrl,connectionProperties = connect_jdbc()
    connectionProperties['truncate'] = 'true'
    writing_df.write.jdbc(url=jdbcUrl, table=table_name, mode='overwrite', properties=connectionProperties)

# COMMAND ----------

# def write_on_asql_append(existing_df,table_name):
#     jdbcUrl,connectionProperties = connect_jdbc()
#     existing_df.write.jdbc(url=jdbcUrl, table=table_name, mode='append', properties=connectionProperties)

# COMMAND ----------

## MAIN PROCESS
final_df_list = []
for each_page in in_page_list:
    raw_filepath = '/mnt/test-container-01/delisted_companies/'+as_of_dt+'/page'+each_page+'.json'
    check_path_from_raw(raw_filepath)
    insert_df = spark.read.option("multiline","true").json(raw_filepath)

    final_df_list.append(insert_df)

final_df = reduce(DataFrame.unionAll, final_df_list)

display(final_df)
# adjust column type
final_df = final_df.withColumn("ipoDate", col("ipoDate").cast(DateType()))
final_df = final_df.withColumn("delistedDate", col("delistedDate").cast(DateType()))

# load the data in asql
write_on_asql_overwrite(final_df,asql_table_name)

# COMMAND ----------

display(query_asql(asql_table_name))

# COMMAND ----------

## EXIT NOTEBOOK

dbutils.notebook.exit(json.dumps({'status': 'SUCCEEDED'}))