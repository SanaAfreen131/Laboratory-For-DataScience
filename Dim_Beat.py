import pyodbc
import csv

# Database connection details
server = 'tcp:lds.di.unipi.it'
username = 'Group_ID_31'
password = 'RAKNWST8'
database = 'Group_ID_31_DB'

# Connection string
connectionString = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
cnxn = pyodbc.connect(connectionString)
cursor = cnxn.cursor()

# Define SQL INSERT query 
sql = """
    INSERT INTO Dim_Beat (RD_NO, BEAT_OF_OCCURRENCE)
    VALUES (?, ?)
"""

# Path to the CSV file
file_path = r'C:\Users\Lenovo\OneDrive\Desktop\Data Science\Decision support project\LDS24 - Data\Dim_Beat.csv'

# Initialize counters
successful_inserts = 0
failed_inserts = 0
failed_records = []

try:
    # Read data into a list for batch insertion
    data = []
    with open(file_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)  # Skip the header row
        for row in csv_reader:
            rd_no = row[0] if row[0] else None
            beat_of_occurrence = row[1] if row[1] else None
            data.append((rd_no, beat_of_occurrence))

    # Perform batch insertion
    cursor.executemany(sql, data)
    cnxn.commit()

    # Successful insertion count
    successful_inserts = len(data)
    print(f"Successfully inserted {successful_inserts} records.")

except Exception as e:
    # Handle errors during batch insertion
    print(f"Error during batch insert: {e}")
    failed_inserts = len(data)  # Assume all records failed if the batch fails
    failed_records.extend(data)

finally:
    # Display summary
    print(f"Total Records Processed: {successful_inserts + failed_inserts}")
    print(f"Records Successfully Inserted: {successful_inserts}")
    print(f"Records Failed to Insert: {failed_inserts}")

    # Close the cursor and connection
    cursor.close()
    cnxn.close()

    # Optionally, save failed records to a file for further inspection
    if failed_records:
        failed_file_path = "failed_records.csv"
        with open(failed_file_path, mode='w', encoding='utf-8', newline='') as failed_file:
            csv_writer = csv.writer(failed_file)
            csv_writer.writerow(['RD_NO', 'BEAT_OF_OCCURRENCE'])  # Header
            csv_writer.writerows(failed_records)
        print(f"Failed records saved to {failed_file_path}")
