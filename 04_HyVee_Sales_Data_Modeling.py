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

sqlexe.execute_sql_file('Calendar_Dimension.sql', connection_params)
sqlexe.execute_sql_file('Data_Modeling.sql', connection_params)