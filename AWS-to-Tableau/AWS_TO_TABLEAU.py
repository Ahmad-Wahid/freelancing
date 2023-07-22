# ==================================================
#                Instructions
# ==================================================
"""
    * Before the user try to run the script, make sure that user has installed 
        following python libraries.
            - arrow
            - pandas
            - tableauhyperapi
            - tableauserverclient
            - tableau_api_lib
        
    * Once all required libraries are installed, in section 2 user needs to provide the given credentials 
        and give name of the hyper file. 
    
    * In section 5, preprocessing can done according to the data. So the user needs to update it for 
        some different data source. 
     
    * Creating Hyper File Methods: (section 6)
        - CREATE_AND_REPLACE, CREATE_OR_REPLACE, CREATE_AND_APPEND, CREATE_OR_APPEND or CREATE_ONLY
    
    * Publishing Data Source Methods: (section 7)
        - Overwrite, Append, or CreateNew

"""


#   <----------- Section 1 ------------->
# ==================================================
#                 Import Required Libraries
# ==================================================
import json
import requests
import arrow
import datetime
import pandas as pd
from pandas import json_normalize

import tableauhyperapi
import tableauserverclient as TSC
from tableauhyperapi import (
    HyperProcess,
    Connection,
    TableDefinition,
    SqlType,
    Telemetry,
    Inserter,
    CreateMode,
    TableName,
)
from tableau_api_lib import TableauServerConnection


#   <----------- Section 2 ------------->
# ==================================================
#                 USER CREDENTIALS
# ==================================================

# Access by using user email and password
SERVER = "https://anything.com"
USER_NAME = "user-name"
PASSWORD = "1234abc#@!"
SITE_NAME = "site-name"
PROJECT_ID = "project-id"

# Access by using PersonalAccessToken
TOKEN_NAME = "token-name"
TOKEN_VALUE = "token"

# this file is stored locally, .hyper extension is must.
FILE_PATH = "data.hyper"


#   <----------- Section 3 ------------->
# ==================================================
#            Make Connection to Tableau Server
# ==================================================

# create a instance of server
server = TSC.Server(SERVER)

# set the version number > 2.3
# the server_info.get() method works in 2.4 and later
server.version = "3.0"

s_info = server.server_info.get()
API_VERSION = s_info.rest_api_version

# create configuration for tableau server
tableau_server_config = {
    "my_env": {
        "server": SERVER,
        "api_version": API_VERSION,
        "username": USER_NAME,
        "password": PASSWORD,
        "site_name": SITE_NAME,
        "site_url": SITE_NAME,
    }
}

# connect to the tableau server
tconn = TableauServerConnection(tableau_server_config, env="my_env")
response = tconn.sign_in()

if response.ok:
    print("Connection is ready!")
else:
    print("Please Check your connection!")


#   <----------- Section 4 ------------->
# ==================================================
#               Import Data from Database
# ==================================================
print("Importing Data...")
# API URL
url = "api-url"

# specify the payload
payload = {}
# specify the header
headers = {}

# call the API and get a response, using the requests.request function
response = requests.request("GET", url, headers=headers, data=payload)

# transform the response data to JSON
response = response.json()

# transform the JSON response to a dataframe using the json_normalize function in Pandas
df = json_normalize(response)

# For checking the daraframes
# print(df)

# save the data frame as CSV
df.to_csv(r"./df.csv", encoding="utf-8", index=False)

# If we are planning to upload on tableau server directly then we need .tde file
# df.to_csv(r'./df.tde', encoding='utf-8', index=False)

print("Data is imported")


#   <----------- Section 5 ------------->
# ==================================================
#                 Preprocessing
# ==================================================
""" 
    Preprocessing is required because we need to have a consistent datatype 
    for each column. For example, customerCity's datatype is string but it also
    contains Null values, so before creating a hyper file we have to fill null
    values with some empty string.

    Similarly, numeric values should not contain empty strings so that we have to
    use pd.to_numeric to convert a string value to an integer and keep null strings 
    as null values.
"""

# convert string values to integer
df["price"] = pd.to_numeric(df["price"], errors="coerce")
df["alcPrice"] = pd.to_numeric(df["alcPrice"], errors="coerce")
df["upc"] = pd.to_numeric(df["upc"], errors="coerce")
df["size"] = pd.to_numeric(df["size"], errors="coerce")

# fill null values with empty string
df["customerCity"] = df["customerCity"].fillna("")
df["customerCountry"] = df["customerCountry"].fillna("")
df["customerState"] = df["customerState"].fillna("")
df["shipToCity"] = df["shipToCity"].fillna("")
df["companyName"] = df["companyName"].fillna("")

# convert string date to pandas datetime
df["orderDate"] = pd.to_datetime(df["orderDate"])

print("Preprocessing is completed.")


#   <----------- Section 6 ------------->
# ==================================================
#          Create Hyper File From DataFrame
# ==================================================
"""
    Following datatypes are used:
    1- text => string
    2- double => float
    3- int => integer
    4- Date => date

"""


df_dict = df.to_dict(orient="list")

print("Creating Hyper file...")

# Step 1: Start a new private local Hyper instance
with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU, "myapp") as hyper:
    # Step 2:  Create the the .hyper file, replace it if it already exists
    # define any create method accordingly - CREATE_AND_REPLACE, CREATE_OR_REPLACE
    # CREATE_AND_APPEND, CREATE_OR_APPEND or CREATE_ONLY
    with Connection(
        endpoint=hyper.endpoint,
        create_mode=CreateMode.CREATE_AND_REPLACE,
        database=FILE_PATH,
    ) as connection:
        # Step 3: Create the schema
        connection.catalog.create_schema("Extract")
        # Step 4: Create the table definition
        columns = []

        for col_name, col_values in df_dict.items():
            col_type = SqlType.text()  # Default type is text
            # Determine the SQL type based on the type of the first value in the column
            if isinstance(col_values[0], int):
                col_type = SqlType.int()
            elif isinstance(col_values[0], float):
                col_type = SqlType.double()
            elif isinstance(col_values[0], str):
                col_type = SqlType.text()
            elif isinstance(col_values[0], datetime.datetime):
                col_type = SqlType.date()
            # add column and its type to table_definition
            columns.append(TableDefinition.Column(col_name, col_type))

        schema = TableDefinition(
            table_name=TableName("Extract", "Extract"), columns=columns
        )

        # Step 5: Create the table in the connection catalog
        connection.catalog.create_table(schema)
        with Inserter(connection, schema) as inserter:
            for index, row in df.iterrows():
                inserter.add_row(row)
            inserter.execute()

print("Hyper file is created!")


#   <----------- Section 7 ------------->
# ==================================================
#             Publish Datasource
# ==================================================


# Tableau Authentication by username and password
tableau_auth = TSC.TableauAuth(
    USER_NAME, PASSWORD, user_id_to_impersonate=None, site_id=SITE_NAME
)
server = TSC.Server(SERVER, use_server_version=True)

# Tableau Authentication by Personal Access Token
# tableau_auth = TSC.PersonalAccessTokenAuth(TOKEN_NAME, TOKEN_VALUE, site_id=SITE_NAME)
# server = TSC.Server(SERVER, use_server_version=True)

# sign in server
with server.auth.sign_in(tableau_auth):
    # Create a new datasource item to publish - empty project_id field
    # will default the publish to the site's default project
    new_datasource = TSC.DatasourceItem(project_id=PROJECT_ID)
    # Define publish mode - Overwrite, Append, or CreateNew
    publish_mode = TSC.Server.PublishMode.Overwrite
    # Publish the datasource.
    new_datasource = server.datasources.publish(new_datasource, FILE_PATH, publish_mode)

    publish_time = str(arrow.now().datetime).split(".")[0]
    print(f"Your datasource is published at {publish_time}")
