import os
from datetime import datetime, date
import pandas as pd
import pymysql

# Get today's date in the format YYYY_MM_DD
today_date = str(date.today()).replace('-', '_')

# Connect to the MySQL database with error handling
try:
    conn = pymysql.Connect(
        host='localhost',
        user='root',
        password='actowiz',
        database='zepto_'
    )
except pymysql.MySQLError as e:
    print(f"Error connecting to the database: {e}")
    exit(1)

# Query data from Zepto and Blinkit tables
try:
    query1 = f'''
        SELECT platform, date, pincode, city, competitor_brand_name, 
               competitor_sku_name, competitor_sku, competitor_url, 
               comp_instock, competitor_mrp, competitor_selling_price
        FROM zepto_.zepto_comp_data_table_{today_date}
    '''
    query2 = f'''
        SELECT platform, date, pincode, city, competitor_brand_name, 
               competitor_sku_name, competitor_sku, competitor_url, 
               comp_instock, competitor_mrp, competitor_selling_price
        FROM blinkit_.blinkit_comp_data_table_{today_date}
    '''

    df1 = pd.read_sql(query1, conn)
    df2 = pd.read_sql(query2, conn)
except Exception as e:
    print(f"Error executing queries: {e}")
    conn.close()
    exit(1)

# Concatenate DataFrames and add an ID column
df = pd.concat([df1, df2], ignore_index=True)
df.insert(0, 'id', range(1, len(df) + 1))

# Define folder and initial file paths
folder_path = "C:\\Shalu\\data_files\\weekly_files"
os.makedirs(folder_path, exist_ok=True)  # Ensure the folder exists

# Generate file path with timestamp to avoid conflicts
# timestamp = datetime.now()
file_name = f"comp_data{today_date}.xlsx"
file_path = os.path.join(folder_path, file_name)

# Attempt to save the DataFrame to Excel
try:
    df.to_excel(file_path, index=False)
    print(f'File created: {file_path}')
except PermissionError as e:
    print(f"Permission denied: {e}. Retrying with a new filename...")

    # Retry with a unique timestamped filename
    new_file_name = f"comp_data{today_date}.xlsx"
    new_file_path = os.path.join(folder_path, new_file_name)

    try:
        df.to_excel(new_file_path, index=False)
        print(f'File successfully saved: {new_file_path}')
    except Exception as ex:
        print(f"Failed to save the Excel file: {ex}")

# Close the database connection
conn.close()
