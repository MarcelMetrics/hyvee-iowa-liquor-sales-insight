# %%
# !pip install --upgrade pip
# !pip install pandas
# !pip install sodapy
# !pip install pymysql

# %%
import pandas as pd
from sodapy import Socrata
import json
from datetime import datetime, timedelta
import pymysql

# %%
# Socrata API 
with open('config\socrata_config.json') as f:
  socrata_config = json.load(f)

AppToken = socrata_config['app_token']
UserName = socrata_config['user_name']
Password = socrata_config["password"]

client = Socrata("data.iowa.gov",
                 AppToken,
                 username = UserName,
                 password = Password,
                 timeout=30)

# %%
# MySQL connection settings
with open('config\mysql_config.json') as f:
  mysql_config = json.load(f)

host = mysql_config['hostname']
user = mysql_config['username']
password = mysql_config['password']

# Establish connections to both STG_HYVEE and INT_HYVEE databases
conn_stg = pymysql.connect(host=host, user=user, password=password, db='STG_HYVEE')
cursor_stg = conn_stg.cursor()

conn_int = pymysql.connect(host=host, user=user, password=password, db='INT_HYVEE')
cursor_int = conn_int.cursor()

# %%
# Placeholder and data type conversion dictionaries
with open('dicts/placeholders.json', 'r') as f:
    placeholders = json.load(f)

with open('dicts/num_col_dtype_map.json', 'r') as f:
    num_col_dtype_map = json.load(f)

# %%
# Function for extracting data via Socrata API
def extract_data(client, year, batch_size, offset):
    start_date = f"{year}-01-01T00:00:00.000"
    end_date = f"{year + 1}-01-01T00:00:00.000"
    results = client.get("m3tr-qhgy",
                         select=col_selected, 
                         where=f"LOWER(name) LIKE '%hy-vee%' AND date >= '{start_date}' AND date < '{end_date}'", 
                         limit=batch_size, 
                         offset=offset)
    return results

# %%
# Function for transforming data 
def transform_data(df):
    # df = df.drop_duplicates()
    df.fillna(placeholders, inplace=True)
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    for col, col_type in num_col_dtype_map.items():
        if col_type == 'int':
            df[col] = df[col].astype(float).astype(int)
        else:
            df[col] = df[col].astype(float)
    df = df[(df['state_bottle_cost'] > 0) & (df['state_bottle_retail'] > 0) & (df['sale_bottles'] > 0)]
    return df

# %%
# Function for loading data to a MySQL database
def load_data(conn, cursor, df, batch_size, sql_insert_query):
    data_tuples = list(df.itertuples(index=False, name=None))
    for batch in [data_tuples[i:i + batch_size] for i in range(0, len(data_tuples), batch_size)]:
        for row in batch:
            try:
                cursor.execute(sql_insert_query, row)
            except pymysql.err.IntegrityError as e:
                if 'Duplicate entry' in str(e):
                    # Log the error and skip the duplicated row
                    print(f"Duplicate entry skipped: {row}")
                    continue
                else:
                    raise
        conn.commit()

# %%
load_sql = """
INSERT INTO sales (
    invoice_line_no, date, store, name, city, zipcode, county, 
    category, category_name, vendor_no, vendor_name, itemno, 
    im_desc, state_bottle_cost, state_bottle_retail, sale_bottles
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# %%
col_selected = """
invoice_line_no, date, store, name, city, zipcode, county, 
category, category_name, vendor_no, vendor_name, itemno, im_desc, 
state_bottle_cost, state_bottle_retail, sale_bottles
"""

# %%
# ETL pipeline
start_year = 2021
current_year = datetime.now().year
batch_size = 10000  

for year in range(start_year, current_year + 1):
    offset = 0
    more_data = True

    while more_data:
        results = extract_data(client, year, batch_size, offset)

        if not results:
            more_data = False
        else:
            offset += len(results)
            df = pd.DataFrame.from_records(results)
            df_transformed = transform_data(df)

            load_data(conn_stg, cursor_stg, df_transformed, batch_size, load_sql)
            load_data(conn_int, cursor_int, df_transformed, batch_size, load_sql)

cursor_stg.close()
conn_stg.close()

cursor_int.close()
conn_int.close()