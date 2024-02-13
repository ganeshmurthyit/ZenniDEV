# Databricks notebook source
import requests
import json

# COMMAND ----------

context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
databricks_host = "https://" + context['tags']['browserHostName']
print(f"databricks host is: {databricks_host}")

#Account Id 
accountId = context["tags"]["accountId"]
print(f"account Id is: {accountId}")

# COMMAND ----------

def check_scope():
    #check whether the scope exist before deleting 
    scopes = dbutils.secrets.listScopes()
    for scope_name in scopes:
        if scope_name.name == "UC_upgrade":
            print(f"scope name --> {scope_name.name} EXISTS")
            return True,scope_name.name
    else:
        return False,None
        

# COMMAND ----------

def delete_scope(scope_name):
    data = str(json.dumps({"scope":scope_name}))
    url = '{}/api/2.0/secrets/scopes/delete'.format(databricks_host)
    response = requests.post(url, data=data, auth=(dbutils.secrets.get(scope="UC_upgrade", key="username"), dbutils.secrets.get(scope="UC_upgrade", key="password")))
    dct = response.json()
    if not dct:
        print("Scope deleted successfully..!!")

# COMMAND ----------

def check_token():
    url = '{}/api/2.0/token/list'.format(databricks_host)
    response = requests.get(url, auth=(dbutils.secrets.get(scope="UC_upgrade", key="username"), dbutils.secrets.get(scope="UC_upgrade", key="password")))
    dct = response.json()
    for token_name in dct["token_infos"]:
        if "UC_upgrade" in token_name["comment"]:
            return token_name["token_id"]
    else:
        print("Token not found with UC_upgrade")
        


def delete_token(token_id):
    data = str(json.dumps({"token_id":token_id}))
    url = '{}/api/2.0/token/delete'.format(databricks_host)
    response = requests.post(url, data=data, auth=(dbutils.secrets.get(scope="UC_upgrade", key="username"), dbutils.secrets.get(scope="UC_upgrade", key="password")))
    dct = response.json()
    if not dct:
        print("Token deleted successfully..!!")

# COMMAND ----------

token_id = check_token()
print(f"token id is: {token_id}")

#delete the token
delete_token(token_id)


exists,scope_name = check_scope()
if exists:
    #delete_scope(scope_name)
    pass
else:
    print("scope does not exist. Run setup script to create scope")

# COMMAND ----------


