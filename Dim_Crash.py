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
    INSERT INTO Dim_Crash (RD_NO, POSTED_SPEED_LIMIT, TRAFFIC_CONTROL_DEVICE, DEVICE_CONDITION, WEATHER_CONDITION, LIGHTING_CONDITION, ROADWAY_SURFACE_COND, FIRST_CRASH_TYPE, TRAFFICWAY_TYPE, ALIGNMENT, STREET_NO, STREET_NAME, STREET_DIRECTION, CRASH_TYPE, LATITUDE, LONGITUDE, LOCATION, ROAD_DEFECT, FIRST_CONTACT_POINT, REPORT_TYPE)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# Read data from the local file
file_path = r'C:\Users\Lenovo\OneDrive\Desktop\Data Science\Decision support project\LDS24 - Data\Dim_Crash.csv'  

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
        posted_speed_limit = row[1] if row[1] else None
        traffic_control_device = row[2] if row[2] else None
        device_condition = row[3] if row[3] else None
        weather_condition = row[4] if row[4] else None
        lighting_condition = row[5] if row[5] else None
        roadway_surface_cond = row[6] if row[6] else None
        first_crash_type = row[7] if row[7] else None
        trafficway_type = row[8] if row[8] else None
        alignment = row[9] if row[9] else None
        street_no = row[10] if row[10] else None
        street_name = row[11] if row[11] else None
        street_direction = row[12] if row[12] else None
        crash_type = row[13] if row[13] else None
        latitude = row[14] if row[14] else None
        longitude = row[15] if row[15] else None
        location = row[16] if row[16] else None
        road_defect = row[17] if row[17] else None
        first_contact_point = row[18] if row[18] else None
        report_type = row[19] if row[19] else None
        data.append((rd_no, posted_speed_limit, traffic_control_device, device_condition, weather_condition, lighting_condition, roadway_surface_cond, first_crash_type, trafficway_type, alignment, street_no, street_name, street_direction, crash_type, latitude, longitude, location, road_defect, first_contact_point, report_type))

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
              csv_writer.writerow(['RD_NO', 'POSTED_SPEED_LIMIT', 'TRAFFIC_CONTROL_DEVICE', 'DEVICE_CONDITION', 'WEATHER_CONDITION', 'LIGHTING_CONDITION', 'ROADWAY_SURFACE_COND', 'FIRST_CRASH_TYPE', 'TRAFFICWAY_TYPE', 'ALIGNMENT', 'STREET_NO', 'STREET_NAME', 'STREET_DIRECTION', 'CRASH_TYPE', 'LATITUDE', 'LONGITUDE', 'LOCATION', 'ROAD_DEFECT', 'FIRST_CONTACT_POINT', 'REPORT_TYPE'])  # Header
              csv_writer.writerows(failed_records)
          print(f"Failed records saved to {failed_file_path}")
