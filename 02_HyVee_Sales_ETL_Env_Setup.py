# !pip install cryptography
import pymysql
import json
import sqlexe

with open('config\mysql_config.json') as f:
  mysql_config = json.load(f)

connection_params = {
    'host': mysql_config['hostname'],
    'user': mysql_config['username'],
    'password': mysql_config['password'],
}

sqlexe.execute_sql_file('ETL_Env_Setup.sql', connection_params)