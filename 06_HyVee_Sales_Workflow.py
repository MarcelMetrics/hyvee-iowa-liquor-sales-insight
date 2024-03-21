import subprocess
import datetime
from datetime import date
import pandas as pd
import os
import json
import pymysql

script_filenames = [
    # Execute ETL_Env_Setup only in initial attempt
    '02_HyVee_Sales_ETL_Env_Setup.py',
    '03_HyVee_Sales_ETL_Pipeline.py',
    '04_HyVee_Sales_Data_Modeling.py',
    '05_HyVee_Sales_Feature_Engineering.py'
]

# Initialize an empty list to store log data
log_data = []
process_start_time = datetime.datetime.now()

for filename in script_filenames:
    try:
        subprocess.run(['python', filename], check=True)
        success = True
    except subprocess.CalledProcessError:
        success = False

process_end_time = datetime.datetime.now()

# Calculate duration and format it
total_process_duration = (process_end_time - process_start_time).total_seconds()

def format_duration(duration_seconds):
    hours, remainder = divmod(duration_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    # Format as "HH:MM:SS.sss"
    return f"{int(hours):02d}:{int(minutes):02d}:{seconds:06.3f}"

total_process_duration = format_duration(total_process_duration)
total_process_duration

# Fetch data-related info from database
with open('config\mysql_config.json') as f:
  mysql_config = json.load(f)

db_config = {
    'host': mysql_config['hostname'],
    'user': mysql_config['username'],
    'password': mysql_config['password'],
    'db': 'STG_HYVEE'
}

def get_db_connection():
    return pymysql.connect(**db_config)

def fetch_record_count():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT COUNT(*) FROM sales"
            cursor.execute(sql)
            (count,) = cursor.fetchone()
            return count
    finally:
        conn.close()

record_count = fetch_record_count()

def fetch_data_date_range():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT MIN(date), MAX(date) FROM sales"
            cursor.execute(sql)
            start_date, end_date = cursor.fetchone()
            return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
    finally:
        conn.close()

data_start_date, data_end_date = fetch_data_date_range()

# Calculate New Records Added
csv_filename = 'execution_log_summary.csv'

if os.path.exists(csv_filename):
    # If the CSV exists, read it to get the last execution's record count
    df_previous_logs = pd.read_csv(csv_filename)
    
    if not df_previous_logs.empty:
        last_execution_record_count = df_previous_logs.iloc[-1]['Record Count']
        new_records_added = record_count - last_execution_record_count
    else:
        # If the CSV exists but is empty (unlikely, but to be safe)
        new_records_added = 0
else:
    # If the CSV does not exist, this is the initial execution
    new_records_added = 0

# Add overall process metrics to the log
log_data.append({
    'Execution Date': date.today(),
    'Process Start Time': process_start_time.strftime('%Y-%m-%d %H:%M:%S'),
    'Process End Time': process_end_time.strftime('%Y-%m-%d %H:%M:%S'),
    'Total Duration': total_process_duration,
    'Data Start Date': data_start_date,
    'Data End Date': data_end_date,
    'Record Count': record_count,
    'New Records Added': new_records_added
})

# Create a DataFrame from the log data
df_logs = pd.DataFrame(log_data)


# Load to csv file
# Check if the CSV file already exists to determine if the header should be written
csv_file_exists = os.path.isfile(csv_filename)

# Append the log DataFrame to the CSV file, write header only if the file doesn't exist
df_logs.to_csv(csv_filename, mode='a', header=not csv_file_exists, index=False)

# Read the updated CSV to ensure it includes all appended data
df_logs_updated = pd.read_csv(csv_filename)



# Convert the updated DataFrame to Markdown
def df_to_md(df):
    # Add the header row
    markdown_table = "| " + " | ".join(df.columns) + " |\n"
    
    # Add the separator row
    markdown_table += "| " + " | ".join(["---"] * len(df.columns)) + " |\n"
    
    # Add data rows
    for index, row in df.iterrows():
        markdown_str = "| " + " | ".join(str(value) for value in row) + " |"
        markdown_table += markdown_str + "\n"
    
    return markdown_table

markdown_table = df_to_md(df_logs_updated)

md_title = "# Execution Log Summary\n\n"

markdown_content = md_title + markdown_table

markdown_filename = 'execution_log_summary.md'

with open(markdown_filename, 'w') as md_file:
    md_file.write(markdown_content)
