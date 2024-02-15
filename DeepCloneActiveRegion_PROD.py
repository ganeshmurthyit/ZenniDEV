# Databricks notebook source
"""

FileName: DeepCloneActiveRegion
Author: Koantek
AsOfDate: 2024-01-22
Current Code Version: 1.0.1

"""

# COMMAND ----------

pip install azure.monitor.opentelemetry

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
import re
import json
from datetime import datetime
import pytz
from logging import INFO, getLogger
from azure.monitor.opentelemetry import configure_azure_monitor
import sys
from pyspark.sql.functions import asc, col, lit, split, when



# COMMAND ----------


# LIST OF CATALOGS TO BACKUP. 
LIST_OF_CATAlOG = ["drdbs","drdbs502","drdbs501","drdbs25","drdbs250"]
SCHEMA_TO_INCLUDE = ["drdbs.sample25","drdbs.sample250","drdbs.sample251","drdbs.sample501","drdbs25.sample25","drdbs25.sample251","drdbs25.sample252","drdbs250.sample250","drdbs501.sample501","drdbs502.sample502"]

#Values to connect to ADLS

statusextlocation = dbutils.jobs.taskValues.get(taskKey = "DeepCloneActiveRegion_Pre_Condition", key = "statusextlocation", default = 0, debugValue = 0) 
ddlscriptextlocation = dbutils.jobs.taskValues.get(taskKey = "DeepCloneActiveRegion_Pre_Condition", key = "ddlscriptextlocation", default = 0, debugValue = 0) 

azure_blobstorage_name = "ascenduc" #[REPLACE]
statusContainerName = "deepclonestatus" #[REPLACE]
ContainerName = "ddlscripts1" #[REPLACE]

#statusextlocation = f"abfss://{statusContainerName}@{azure_blobstorage_name}.dfs.core.windows.net/" 
#ddlscriptextlocation = f"abfss://{ContainerName}@{azure_blobstorage_name}.dfs.core.windows.net/"

#Get secrets from Databricks secretScope
ENCODED_AUTH_KEY = dbutils.secrets.get(scope = "deepclone", key = "InstrumentationKey") #[REPLACE]
ENCODED_STORAGE_KEY = dbutils.secrets.get(scope = "deepclone", key = "storagekey") #[REPLACE]

#Configuration for Logger
configure_azure_monitor(
    # Set logger_name to the name of the logger you want to capture logging telemetry with
    connection_string="InstrumentationKey=" + ENCODED_AUTH_KEY
)

logger = getLogger("DeepCloneActiveRegion")
logger.setLevel(INFO)

def get_catalogs_and_databases(LIST_OF_CATAlOG):
    
    df_catalogs_and_databases = None
    _df = None

    for catalog in LIST_OF_CATAlOG:
        _df = spark.sql(f'SHOW DATABASES FROM {catalog}')\
            .select(
                lit(catalog).alias('catalog'),
                col('databaseName').alias('database')
            )
        try:
            df_catalogs_and_databases = df_catalogs_and_databases.union(_df)
            
        except AttributeError as e:
            df_catalogs_and_databases = _df
        except Exception as e:
            raise
    
    list_catalogs_and_databases = [
    {
        'catalog': row[0], 
        'database': row[1]
    } 
    for row in df_catalogs_and_databases.collect()
    ]

    df_catalogs_and_databases_and_tables = None
    df_tables = None

    for current_item in list_catalogs_and_databases:
        df_tables = spark.sql(f"SHOW TABLES FROM {current_item['catalog']}.{current_item['database']}")\
            .select(
                lit(current_item['catalog']).alias('catalog'),
                lit(current_item['database']).alias('database'),
                col('tableName').alias('table')
                
            )
        try:
            df_catalogs_and_databases_and_tables = df_catalogs_and_databases_and_tables.union(df_tables)
        except AttributeError as e:
            # Catching this AttributeError: 'NoneType' object has no attribute 'union'
            df_catalogs_and_databases_and_tables = df_tables
        except Exception as e:
            raise
    
    return df_catalogs_and_databases_and_tables

df_catalogs_and_databases_tables = get_catalogs_and_databases(LIST_OF_CATAlOG)


def include_selected_schema(df_pandas):
    
    cat = []
    schema = []
    for item in SCHEMA_TO_INCLUDE:
        x = item.split(".")
        cat.append(x[0])
        schema.append(x[1])
    
    filtered_df = df_pandas.where(df_pandas.catalog.isin(cat) & df_pandas.database.isin(schema))  
    
    return filtered_df

def filter_dbs():

    df_pandas = df_catalogs_and_databases_tables.where(df_catalogs_and_databases_tables.database!='information_schema')
    if len(SCHEMA_TO_INCLUDE) > 0:
        res_schema = include_selected_schema(df_pandas)

    list_custom = []
    dataCollect = res_schema.collect()
    for row in dataCollect:
        EACH_ROW = row['catalog'] + "." +row['database'] + "." +row['table']
        list_custom.append(EACH_ROW)
    list_custom = np.array(list_custom)
        
    return list_custom

df_filter = filter_dbs()

def getLocation(tblLocation):
    logger.info("getLocation function started")
    match = re.search(r'abfss:[^}]+', tblLocation)
    if match:
        # Get the matched substring
        filtered_data = match.group(0)

        # Replace "\\" with a single "\"
        filtered_data = filtered_data.replace("\\\\", "\\")
        
        # Remove all backslashes
        filtered_data = filtered_data.replace("\\", "")

        # Remove the last double quote justbefore the closing curly brace "}"
        filtered_data = filtered_data.rstrip('"')

        return filtered_data
    else:
        # Return an empty string if the pattern is not found
        return ""
    logger.info("getLocation function ended")
    return filtered_data

child = []

def built_ddlscript(json1,tblLocation):
    
    logger.info("built_ddlscript Function started")
    DDL_dict = json.loads(DDL)
    
    result = getLocation(tblLocation)
    # Extract the string from the JSON
    stmt = DDL_dict["createtab_stmt"]["0"]
    # Remove everything before "CREATE TABLE"
    stmt = stmt[stmt.index("CREATE TABLE"):]
    # Remove everything including and after "COMMENT"
    stmt = stmt[:stmt.index("delta") + len("delta")]
    # Remove all "\n"
    stmt = stmt.replace("\n", "")
    # Create the output dictionary with the DDL key and a list of dictionaries as the value
    list_of_child = []
    list_of_child.append({"StorageLocation": result,"tblname": stmt})
       
    for item in list_of_child:
        child.append(item)
    output_dict = {"DDL": child}
    logger.info("built_ddlscript Function Finished")

    return output_dict

clean_ddl_parent = []

for t in df_filter:
    logger.info("read each table for loop started")
    
    DDL = spark.sql("SHOW CREATE TABLE " + t).toPandas().to_json() 
    ddl_sub = spark.sql("DESCRIBE TABLE EXTENDED " + t).toPandas()
    tblLocation = ddl_sub[ddl_sub["col_name"] == "Location"].to_json()
    
    clean_ddl = built_ddlscript(DDL,tblLocation)
    clean_ddl_parent.append(clean_ddl)

logger.info("read each table for loop finished")
    
newList = clean_ddl_parent[-1]

def generate_json_write_to_adls():
    logger.info("generate_csv_write_to_adls started")
    result = False
   
    ddldata = json.dumps(newList, indent=2) 
    
    statusdata = str({"DDLGenerated":True,"DeepCloneExecuted":False})
    ddl_generated_timestamp = datetime.now().timestamp()
    
    statusextlocation1 = statusextlocation + "status_" + str(ddl_generated_timestamp) + ".json"
    ddlscriptextlocation1 = ddlscriptextlocation + "ddlscript_" + str(ddl_generated_timestamp) + ".json"
    
    try:
        dbutils.fs.put(ddlscriptextlocation1, ddldata, True)
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

