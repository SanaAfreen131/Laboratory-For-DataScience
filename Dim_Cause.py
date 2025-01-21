import pyodbc
import csv

# Database connection details
server = 'tcp:lds.di.unipi.it'
username = 'Group_ID_31'
password = 'RAKNWST8'
database = 'Group_ID_31_DB'
connection_string = (
    f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
)

# Establish the connection
try:
    cnxn = pyodbc.connect(connection_string)
    cursor = cnxn.cursor()
except pyodbc.Error as db_err:
    print(f"Database connection failed: {db_err}")
    exit()

# Define SQL INSERT query
sql = """
    INSERT INTO Dim_Cause (RD_NO, PRIM_CONTRIBUTORY_CAUSE, SEC_CONTRIBUTORY_CAUSE, DRIVER_ACTION, DRIVER_VISION)
    VALUES (?, ?, ?, ?, ?)
"""

# File path
file_path = r'C:\Users\Lenovo\OneDrive\Desktop\Data Science\Decision support project\LDS24 - Data\Dim_Cause.csv'

# Initialize counters
successful_inserts = 0
failed_inserts = 0
failed_records = []

try:
    # Open and read the file correctly
    with open(file_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)  # Skip the header row
        
        # Read and prepare data for batch insertion
        data = []
        for row in csv_reader:
            rd_no = row[0] if row[0] else None
            prim_cause = row[1] if row[1] else None
            sec_cause = row[2] if row[2] else None
            driver_action = row[3] if row[3] else None
            driver_vision = row[4] if row[4] else None
            data.append((rd_no, prim_cause, sec_cause, driver_action, driver_vision))
        
        # Perform batch insertion
        try:
            cursor.executemany(sql, data)
            cnxn.commit()
            successful_inserts = len(data)
            print(f"Successfully inserted {successful_inserts} records.")
        except pyodbc.Error as insert_err:
            print(f"Error during batch insert: {insert_err}")
            failed_inserts = len(data)
            failed_records.extend(data)
            
except FileNotFoundError:
    print(f"File not found: {file_path}")
except IOError as io_err:
    print(f"I/O error: {io_err}")
except Exception as e:
    print(f"Unexpected error: {e}")

finally:
    # Display summary
    print(f"Total Records Processed: {successful_inserts + failed_inserts}")
    print(f"Records Successfully Inserted: {successful_inserts}")
    print(f"Records Failed to Insert: {failed_inserts}")
    
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if cnxn:
        cnxn.close()

    # Save failed records to a file for further inspection
    if failed_records:
        failed_file_path = "failed_records.csv"
        with open(failed_file_path, mode='w', encoding='utf-8', newline='') as failed_file:
            csv_writer = csv.writer(failed_file)
            csv_writer.writerow(['RD_NO', 'PRIM_CONTRIBUTORY_CAUSE', 'SEC_CONTRIBUTORY_CAUSE', 'DRIVER_ACTION', 'DRIVER_VISION'])  # Header
            csv_writer.writerows(failed_records)
        print(f"Failed records saved to {failed_file_path}")
