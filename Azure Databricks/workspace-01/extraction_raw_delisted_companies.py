# Databricks notebook source
## TEST PARAMS
# AS_OF_DT = 20230628
# STOFCK_SYMBOL = 0,1,2,3,4,5

# COMMAND ----------

## IMPORT LIBRARY
import requests
import json

# COMMAND ----------

## NOTEBOOK PARAMS
dbutils.widgets.text("PAGE", "")
dbutils.widgets.text("AS_OF_DT", "")

## ASSIGN VARAIBLES
in_page = dbutils.widgets.get("PAGE")
in_page_list = in_page.split(',')
as_of_dt = dbutils.widgets.get("AS_OF_DT")

global error_log

# COMMAND ----------

def extract_delisted_companies(page):
    r = requests.get('https://financialmodelingprep.com/api/v3/delisted-companies?page='+page+'&apikey='+dbutils.secrets.get(scope="keyvault_scope_01",key="apikey01"))
    if r.status_code == 200:
        try:
            return r.text
        except SomeError:
            return None
    else:
        error_log += f'Could not load data for page{page}, error with HTTP REQUEST ERROR '+str(r.status_code)

# COMMAND ----------

def write_on_storage_account(json_string, output_path):
    # create the path if it doesn't exist
    dbutils.fs.mkdirs(output_path.rsplit("/", 1)[0])
    with open('/dbfs' +output_path, 'w') as file:
        file.write(json_string)

# COMMAND ----------

## MAIN PROCESS
error_log = ''
for each_page in in_page_list:
    output_filepath = '/mnt/test-container-01/delisted_companies/'+as_of_dt+'/page'+each_page+'.json'
    r_json = extract_delisted_companies(each_page)
    write_on_storage_account(r_json, output_filepath)

# COMMAND ----------

## EXIT NOTEBOOK
if error_log == '':
    dbutils.notebook.exit(json.dumps({'status': 'SUCCEEDED'}))
else:
    dbutils.notebook.exit(json.dumps({'status': 'SUCCEEDED', 'message': error_log}))