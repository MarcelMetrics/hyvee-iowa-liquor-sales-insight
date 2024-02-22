import pandas as pd
import json
from datetime import datetime, timedelta
import pymysql

# MySQL connection settings
with open('config\mysql_config.json') as f:
  mysql_config = json.load(f)

host = mysql_config['hostname']
user = mysql_config['username']
password = mysql_config['password']

# ---------------------------------------

# Store Format: Creating a new column to indicate store formats based on the 'name' column.
## Loading data from db
conn_int = pymysql.connect(host=host, user=user, password=password, db='INT_HYVEE')
cursor_int = conn_int.cursor()

sql_query = "SELECT store_id, store_name FROM stores"

df = pd.read_sql(sql_query, conn_int)

cursor_int.close()
conn_int.close()

## Creating the column
df['store_format'] = df['store_name']

df['store_format'] = (
    df['store_format'].str.replace('HY-VEE', 'HY VEE', case=False, regex=True)
                      .str.replace('C-STORE', 'C STORE', case=False, regex=True)
                      .str.split('/|-', n=1).str[0]
                      .str.replace('[0-9#()]+', '', regex=True)
                      .str.strip()
                      .str.replace('\s+', ' ', regex=True)
)

with open('dicts/store_format_map.json', 'r') as f:
    store_format_map = json.load(f)

replacements_lower = {k.lower(): v for k, v in store_format_map.items()}

def apply_replacements(x):
    x_lower = x.lower()
    for key, value in replacements_lower.items():
        if key in x_lower:
            return value
    if x == 'HY VEE':
        return 'Grocery Store'
    return 'Other' if all(x_lower != val.lower() for val in store_format_map.values()) else x

df['store_format'] = df['store_format'].apply(apply_replacements)

# ---------------------------------------

# Liquor Type: Creating a new column to indicate liquor types based on the 'category' and 'category_code' columns.

## Loading data from db
conn_int = pymysql.connect(host=host, user=user, password=password, db='INT_HYVEE')
cursor_int = conn_int.cursor()

sql_query = "SELECT category_code, category FROM items"

df = pd.read_sql(sql_query, conn_int)

cursor_int.close()
conn_int.close()

## Creating the column
df['liquor_type'] = df['category']

with open('dicts/liquor_type_map.json', 'r') as f:
    liquor_type_map = json.load(f)

def match_liquor_type(number):
    number_str = str(number)[:3] 
    return liquor_type_map.get(number_str, 'Other') 

df['liquor_type'] = df['category_code'].apply(match_liquor_type)

# ---------------------------------------

# Numerical Metrics: Executing the relevant SQL script
import sqlexe

connection_params = {
    'host': mysql_config['hostname'],
    'user': mysql_config['username'],
    'password': mysql_config['password'],
}
sqlexe.execute_sql_file('metrics.sql', connection_params)