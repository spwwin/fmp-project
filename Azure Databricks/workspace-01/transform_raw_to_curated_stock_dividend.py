# Databricks notebook source
## TEST PARAMS
# AS_OF_DT = 20230628
# STOFCK_SYMBOL = AAPL,MSFT,MSB,CALM,HTGC

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
dbutils.widgets.text("STOCK_SYMBOL", "")
dbutils.widgets.text("AS_OF_DT", "")

## ASSIGN VARAIBLES
in_stock_symbol = dbutils.widgets.get("STOCK_SYMBOL")
in_stock_symbol_list = in_stock_symbol.split(',')
as_of_dt = dbutils.widgets.get("AS_OF_DT")
asql_table_name = 'curated.history_dividends'

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
for each_stock_symbol in in_stock_symbol_list:
    raw_filepath = '/mnt/test-container-01/stock_dividend/'+as_of_dt+'/'+each_stock_symbol+'.json'
    check_path_from_raw(raw_filepath)
    json_df = spark.read.option("multiline","true").json(raw_filepath)

    # check if there is an explodable historical column
    is_explodable = json_df.select(expr('size(historical) > 0')).first()[0]
    if is_explodable:
        # explode array from historical column
        exploded_df = json_df.select(
            col("symbol"),
            explode(col("historical")).alias("historical")
        )
        # change the column name back
        insert_df = exploded_df.select(
            col("symbol"),
            col("historical.date").alias("date"),
            col("historical.label").alias("label"),
            col("historical.adjDividend").alias("adjDividend"),
            col("historical.dividend").alias("dividend"),
            col("historical.recordDate").alias("recordDate"),
            col("historical.paymentDate").alias("paymentDate"),
            col("historical.declarationDate").alias("declarationDate")
        )
        final_df_list.append(insert_df)

final_df = reduce(DataFrame.unionAll, final_df_list)

# adjust column type
final_df = final_df.withColumn("date", col("date").cast(DateType()))
final_df = final_df.withColumn("adjDividend", col("adjDividend").cast(DoubleType()))
final_df = final_df.withColumn("dividend", col("dividend").cast(DoubleType()))
final_df = final_df.withColumn("recordDate", col("recordDate").cast(DateType()))
final_df = final_df.withColumn("paymentDate", col("paymentDate").cast(DateType()))
final_df = final_df.withColumn("declarationDate", col("declarationDate").cast(DateType()))

# load the data in asql
write_on_asql_overwrite(final_df,asql_table_name)

# COMMAND ----------

display(query_asql(asql_table_name))

# COMMAND ----------

## EXIT NOTEBOOK

dbutils.notebook.exit(json.dumps({'status': 'SUCCEEDED'}))