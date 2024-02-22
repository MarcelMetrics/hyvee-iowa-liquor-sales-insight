import pymysql

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