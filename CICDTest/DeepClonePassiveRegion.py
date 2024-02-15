# Databricks notebook source
"""

FileName: DeepClonePassiveRegion
Author: Koantek
AsOfDate: 2024-01-28
Current Code Version: 1.0.2

"""

# COMMAND ----------

pip install azure.monitor.opentelemetry

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import asyncio
import json
import re
import time
from datetime import datetime
import pytz
import ast
from logging import INFO, getLogger
from azure.monitor.opentelemetry import configure_azure_monitor 
import sys


# COMMAND ----------

#Get secrets

ENCODED_AUTH_KEY = dbutils.secrets.get(scope = "deepclone", key = "InstrumentationKey") #[REPLACE]
ENCODED_STORAGE_KEY = dbutils.secrets.get(scope = "deepclone", key = "storagekey") #[REPLACE]

#Values to connect to ADLS

azure_blobstorage_name = "ascenduc" #[REPLACE]
statusContainerName = "deepclonestatus" #[REPLACE]
ContainerName = "ddlscripts1" #[REPLACE]

#statusextlocation = f"abfss://{statusContainerName}@{azure_blobstorage_name}.dfs.core.windows.net/" 
#ddlscriptextlocation = f"abfss://{ContainerName}@{azure_blobstorage_name}.dfs.core.windows.net/"

statusextlocation = dbutils.jobs.taskValues.get(taskKey = "DeepClonePassiveRegion_Pre_Condition", key = "statusextlocation", default = 0, debugValue = 0) 
ddlscriptextlocation = dbutils.jobs.taskValues.get(taskKey = "DeepClonePassiveRegion_Pre_Condition", key = "ddlscriptextlocation", default = 0, debugValue = 0) 

#Configuration for Logger
configure_azure_monitor(
    # Set logger_name to the name of the logger you want to capture logging telemetry with
    connection_string="InstrumentationKey=" + ENCODED_AUTH_KEY
)

logger = getLogger("DeepClonePassiveRegion")
logger.setLevel(INFO)


# COMMAND ----------


def get_ddlscripts():
    data = ""
    
    ddlfile = dbutils.fs.ls(ddlscriptextlocation)
    
    ddlfile = pd.DataFrame(ddlfile)
    #print("ddlfile",ddlfile)
    latest_modificationtime = max(ddlfile['modificationTime'])
    
    #display(latest_modificationtime)
    statusfile = dbutils.fs.ls(statusextlocation)
    statusfile = pd.DataFrame(statusfile)
    latest_status_modificationtime = max(statusfile['modificationTime'])
    print("statusfile",statusfile)
    
    
    latest_statusfile = statusfile[statusfile["modificationTime"]==latest_status_modificationtime]["path"].values
    print("latest_statusfile",latest_statusfile)
    latest_ddlfile = ddlfile[ddlfile["modificationTime"]==latest_modificationtime]["path"].values
    print("latest_ddlfile",latest_ddlfile)
    
    content = dbutils.fs.head(latest_statusfile[0])
    dict_obj = ast.literal_eval(content)
    
    if dict_obj["DDLGenerated"] == True and dict_obj["DeepCloneExecuted"] == False:
        dbutils.fs.cp(latest_ddlfile[0],"dbfs:/FileStore/tables/perfscript/adls")
        file_name = latest_ddlfile[0].split("/")
        data = pd.read_json("/dbfs/FileStore/tables/perfscript/adls/" + file_name[3])
        #data = dbutils.fs.head(latest_ddlfile[0])
        #data = json.loads(data)
        #data = pd.DataFrame(data)
        
    return data

data = get_ddlscripts()
print(data)

# COMMAND ----------


sample_data = data
leng = len(data)
 
def execute_createscript():
    list_of_cretescripts = []
    
    for i in range(leng):  
        StorageLocation= " LOCATION " + "'"  + sample_data["DDL"][i]["StorageLocation"] + "'"
        tblname = sample_data["DDL"][i]["tblname"]
        index = tblname.find(' (')
        tblname = tblname[:index] + '1' + tblname[index:]
        str = ("".join([tblname, StorageLocation]))
        list_of_cretescripts.append(str)
        i += 1
        
    return list_of_cretescripts

def execute_deepclean():
    
    list_of_deepclonescripts = []
    LIST_OF_TABLES_TO_DROP = []

    for i in range(leng):  
        tblname = sample_data["DDL"][i]["tblname"]
        
        # Extract everything just before first "("
        tblname_without1 = tblname.split('(')[0].strip()

        # Remove "CREATE TABLE "
        tblname_without1 = tblname_without1.replace("CREATE TABLE ", "")

        # append 1 in the last
        tblname_with1 = tblname_without1 + '1'
        
        query = "CREATE OR REPLACE TABLE " + tblname_without1 + " DEEP CLONE " + tblname_with1
        list_of_deepclonescripts.append(query)
        LIST_OF_TABLES_TO_DROP.append(tblname_with1)
        LIST_OF_TABLES_TO_DROP.append(tblname_without1)
    
    return list_of_deepclonescripts,tblname_with1,tblname_without1,LIST_OF_TABLES_TO_DROP

LIST_OF_SCHEMAS = []

def get_schema_if_not_exist():
    result = True
    LIST_OF_TABLES = execute_deepclean()
    
    for item in LIST_OF_TABLES[3]:
        SCHEMA = item.rsplit(".",1)
        LIST_OF_SCHEMAS.append(SCHEMA[0])

    return result

def create_schema_if_not_exist():
    result = True
    get_schema_if_not_exist()
    for item in LIST_OF_SCHEMAS:
        CREATE_SCHEMA_QUERY = "CREATE SCHEMA IF NOT EXISTS " + item
        spark.sql(CREATE_SCHEMA_QUERY)
    return result

def drop_tables():

    result = True
    LIST_OF_TABLES_TO_DROP = execute_deepclean()
    for item in LIST_OF_TABLES_TO_DROP[3]:
        DROP_QUERY = "DROP TABLE IF EXISTS " + item
        spark.sql(DROP_QUERY)

    return result 
 

def build_dictionary():
    drop_tables()
    create_schema_if_not_exist()
    list_of_cretescripts = execute_createscript()
    list_of_deepclonescripts = execute_deepclean()
    
    dictionary_list = [{k: v} for k, v in zip(list_of_cretescripts, list_of_deepclonescripts[0])]
    
    return dictionary_list

async def process_data(d):
    
    key, value = d.popitem()
    
    key = f'"{key}"'
    value = f'"{value}"'
    key = key[1:-1]
    value = value[1:-1]
    
    spark.sql(key)
    spark.sql(value)

async def execute_scripts():
    executed = False
    
    response = build_dictionary()  # store the result of build_dictionary in a variable
    tasks = []
    for d in response:
        task = asyncio.ensure_future(process_data(d))
        tasks.append(task)
        
        await asyncio.sleep(5)  # wait for 1 seconds before starting the next task
    await asyncio.gather(*tasks)
    return executed
    
await execute_scripts()

def generate_json_write_to_adls():
    logger.info("generate_csv_write_to_adls started")
    result = False
   
    statusdata = str({"DDLGenerated":True,"DeepCloneExecuted":True})
    ddl_generated_timestamp = datetime.now().timestamp()
    
    statusextlocation1 = statusextlocation + "status_" + str(ddl_generated_timestamp) + ".json"
    ddlscriptextlocation1 = ddlscriptextlocation + "ddlscript_" + str(ddl_generated_timestamp) + ".json"
    
    try:
        
        dbutils.fs.put(statusextlocation1, statusdata, True)
        logger.info("generate_json_write_to_adls posted ddl sucessfully")
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
            logger.info("generate_json_write_to_adls failed to post")
        else:
            return
        
    logger.info("generate_csv_write_to_adls ended")
    return result

res = generate_json_write_to_adls()


