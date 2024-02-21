import pymysql
import json

def execute_sql_file(filename, connection_params):
    # Initialize the database connection
    db_connection = pymysql.connect(**connection_params)
    cursor = db_connection.cursor()
    
    # Read the SQL script file
    with open(filename, 'r') as sql_file:
        sql_script = sql_file.read()
    
    sql_commands = sql_script.split(';')
    
    for command in sql_commands:
        if command.strip():  # Skip any empty commands resulting from the split
            try:
                cursor.execute(command)
                db_connection.commit()
            except Exception as e:
                # Optional: log the error or handle it otherwise
                print(f"Error executing command: {command}\n{e}")

    cursor.close()
    db_connection.close()



# MySQL connection settings
with open('config\mysql_config.json') as f:
  mysql_config = json.load(f)

host = mysql_config['hostname']
user = mysql_config['username']
password = mysql_config['password']

connection_params = {
    'host': mysql_config['hostname'],
    'user': mysql_config['username'],
    'password': mysql_config['password'],
}

execute_sql_file('Calendar_Dimension.sql', connection_params)
execute_sql_file('Data_Modeling.sql', connection_params)