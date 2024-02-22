import pandas as pd
import json
from datetime import datetime, timedelta
import pymysql
import sqlexe

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

# Implement the changes in database
try:
    conn_int = pymysql.connect(host=host, user=user, password=password, db='INT_HYVEE')
    
    with conn_int.cursor() as cursor_int:
        # Check if the column exists
        cursor_int.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'INT_HYVEE' 
            AND table_name = 'stores' 
            AND column_name = 'store_format';
        """)
        if cursor_int.fetchone():
            # If the column exists, drop it
            cursor_int.execute("ALTER TABLE stores DROP COLUMN store_format;")
        
        # Add the col
        cursor_int.execute("ALTER TABLE stores ADD COLUMN store_format VARCHAR(255);")

        sql_update = "UPDATE stores SET store_format = %s WHERE store_id = %s;"

        for index, row in df.iterrows():
            cursor_int.execute(sql_update, (row['store_format'], row['store_id']))

        conn_int.commit()
except pymysql.Error as e:
    print(f"Database error occurred: {e}")
finally:
    conn_int.close()

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

df = df.drop_duplicates()
df['liquor_type'] = df['category_code'].apply(match_liquor_type)

# Implement the changes in database
try:
    conn_int = pymysql.connect(host=host, user=user, password=password, db='INT_HYVEE')
    with conn_int.cursor() as cursor_int:
        # Check if the 'liquor_type' column exists
        cursor_int.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'INT_HYVEE' 
            AND table_name = 'items' 
            AND column_name = 'liquor_type';
        """)
        # If the column exists, drop it
        if cursor_int.fetchone():
            cursor_int.execute("ALTER TABLE items DROP COLUMN liquor_type;")
        
        # Add the column
        cursor_int.execute("ALTER TABLE items ADD COLUMN liquor_type VARCHAR(255);")

        # Update the col
        sql_update = "UPDATE items SET liquor_type = %s WHERE category_code = %s;"

        for index, row in df.iterrows():
            cursor_int.execute(sql_update, (row['liquor_type'], row['category_code']))

        conn_int.commit()
except pymysql.Error as e:
    print(f"Database error occurred: {e}")
finally:
    conn_int.close()

# ---------------------------------------

# Numerical Metrics: Executing the relevant SQL script

with open('config\mysql_config.json') as f:
  mysql_config = json.load(f)

connection_params = {
    'host': mysql_config['hostname'],
    'user': mysql_config['username'],
    'password': mysql_config['password'],
}

sqlexe.execute_sql_file('metrics.sql', connection_params)