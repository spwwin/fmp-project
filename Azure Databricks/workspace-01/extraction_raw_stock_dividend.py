# Databricks notebook source
## TEST PARAMS
# AS_OF_DT = 20230628
# STOFCK_SYMBOL = AAPL,MSFT,MSB,CALM,HTGC

# COMMAND ----------

## IMPORT LIBRARY
import requests
import json

# COMMAND ----------

## NOTEBOOK PARAMS
dbutils.widgets.text("STOCK_SYMBOL", "")
dbutils.widgets.text("AS_OF_DT", "")

## ASSIGN VARAIBLES
in_stock_symbol = dbutils.widgets.get("STOCK_SYMBOL")
in_stock_symbol_list = in_stock_symbol.split(',')
as_of_dt = dbutils.widgets.get("AS_OF_DT")

global error_log

# COMMAND ----------

def extract_stock_dividend(stock_symbol):
    r = requests.get('https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/'+stock_symbol+'?apikey='+dbutils.secrets.get(scope="keyvault_scope_01",key="apikey01"))
    if r.status_code == 200:
        try:
            return r.text
        except SomeError:
            return None
    else:
        error_log += f'Could not load data for {stock_symbol}, error with HTTP REQUEST ERROR '+str(r.status_code)

# COMMAND ----------

def write_on_storage_account(json_string, output_path):
    # create the path if it doesn't exist
    dbutils.fs.mkdirs(output_path.rsplit("/", 1)[0])
    with open('/dbfs' +output_path, 'w') as file:
        file.write(json_string)

# COMMAND ----------

## MAIN PROCESS
error_log = ''
for each_stock_symbol in in_stock_symbol_list:
    output_filepath = '/mnt/test-container-01/stock_dividend/'+as_of_dt+'/'+each_stock_symbol+'.json'
    r_json = extract_stock_dividend(each_stock_symbol)
    write_on_storage_account(r_json, output_filepath)

# COMMAND ----------

## EXIT NOTEBOOK
if error_log == '':
    dbutils.notebook.exit(json.dumps({'status': 'SUCCEEDED'}))
else:
    dbutils.notebook.exit(json.dumps({'status': 'SUCCEEDED', 'message': error_log}))