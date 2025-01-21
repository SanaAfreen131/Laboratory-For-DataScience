import pyodbc
import csv

# Database connection details
server = 'tcp:lds.di.unipi.it'
username = 'Group_ID_31'
password = 'RAKNWST8'
database = 'Group_ID_31_DB' 

connectionString = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
cnxn = pyodbc.connect(connectionString)

cursor = cnxn.cursor()

# Define SQL INSERT query 
sql = """
    INSERT INTO Dim_Vehicle (RD_NO, VEHICLE_ID, CRASH_UNIT_ID, VEHICLE_TYPE, MAKE, MODEL, VEHICLE_YEAR, LIC_PLATE_STATE, VEHICLE_DEFECT, VEHICLE_USE, OCCUPANT_CNT, UNIT_TYPE, TRAVEL_DIRECTION, UNIT_NO, MANEUVER)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# Read data from the local file
file_path = r'C:\Users\Lenovo\OneDrive\Desktop\Data Science\Decision support project\LDS24 - Data\Dim_Vehicle.csv'  

# Initialize counters
successful_inserts = 0
failed_inserts = 0
failed_records = []

try:
    # Read data into a list for batch insertion
    data = []
    with open(file_path, mode='r', encoding='utf-8') as file:
     csv_reader = csv.reader(file)
     header = next(csv_reader)  # Skip the header row if present
     for row in csv_reader:
        # Ensure data matches the table schema
        rd_no = row[0] if row[0] else None
        vehicle_id = row[1] if row[1] else None
        crash_unit_id = row[2] if row[2] else None
        vehicle_type = row[3] if row[3] else None
        make = row[4] if row[4] else None
        model = row[5] if row[5] else None
        vehicle_year = row[6] if row[6] else None
        lic_plate_state = row[7] if row[7] else None
        vehicle_defect = row[8] if row[8] else None
        vehicle_use = row[9] if row[9] else None
        occupant_cnt = row[10] if row[10] else None
        unit_type = row[11] if row[11] else None
        travel_direction = row[12] if row[12] else None
        unit_no = row[13] if row[13] else None
        maneuver = row[14] if row[14] else None
        data.append((rd_no, vehicle_id, crash_unit_id, vehicle_type, make, model, vehicle_year, lic_plate_state, vehicle_defect, vehicle_use, occupant_cnt, unit_type, travel_direction, unit_no, maneuver))

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
              csv_writer.writerow(['RD_NO', 'VEHICLE_ID', 'CRASH_UNIT_ID', 'VEHICLE_TYPE', 'MAKE', 'MODEL', 'VEHICLE_YEAR', 'LIC_PLATE_STATE', 'VEHICLE_DEFECT', 'VEHICLE_USE', 'OCCUPANT_CNT', 'UNIT_TYPE', 'TRAVEL_DIRECTION', 'UNIT_NO', 'MANEUVER'])  # Header
              csv_writer.writerows(failed_records)
          print(f"Failed records saved to {failed_file_path}")
