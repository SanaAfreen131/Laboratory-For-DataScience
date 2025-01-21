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

# Initialize default keys to start from 1
current_date_key = 1
current_beat_key = 1
current_cause_key = 1
current_vehicle_key = 1
current_client_key = 1
current_crash_key = 1

# Define SQL INSERT query for Fact_Crash 
sql = """
    INSERT INTO Fact_Crash (
        RD_NO, DateKey, BeatKey, CauseKey, VehicleKey, ClientKey, CrashKey,
        INJURIES_TOTAL, INJURIES_FATAL, INJURIES_INCAPACITATING, INJURIES_REPORTED_NOT_EVIDENT,
        NUM_UNITS, DAMAGE, DAMAGE_CATEGORY, INJURIES_NO_INDICATION, INJURIES_UNKNOWN,
        INJURIES_NON_INCAPACITATING, MOST_SEVERE_INJURY
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# File path to your local CSV file
file_path = r'C:\Users\Lenovo\OneDrive\Desktop\Data Science\Decision support project\LDS24 - Data\Fact_Crash.csv'

# Read and process the CSV file
try:
    data = []
    with open(file_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)  # Skip the header row

        for row in csv_reader:
            # Assign and increment default keys for each new record
            rd_no = row[0]  
            date_key = current_date_key
            beat_key = current_beat_key
            cause_key = current_cause_key
            vehicle_key = current_vehicle_key
            client_key = current_client_key
            crash_key = current_crash_key

            # Increment the keys for the next record
            current_date_key += 1
            current_beat_key += 1
            current_cause_key += 1
            current_vehicle_key += 1
            current_client_key += 1
            current_crash_key += 1

            # Map remaining CSV columns to corresponding table columns
            injuries_total = row[1]
            injuries_fatal = row[2]
            injuries_incapacitating = row[3]
            injuries_reported_not_evident = row[4]
            num_units = row[5]
            damage = row[6]
            damage_category = row[7]
            injuries_no_indication = row[8]
            injuries_unknown = row[9]
            injuries_non_incapacitating = row[10]
            most_severe_injury = row[11]

            # Append to data for batch insertion
            data.append((
                rd_no, date_key, beat_key, cause_key, vehicle_key, client_key, crash_key,
                injuries_total, injuries_fatal, injuries_incapacitating, injuries_reported_not_evident,
                num_units, damage, damage_category, injuries_no_indication, injuries_unknown,
                injuries_non_incapacitating, most_severe_injury
            ))

    # Perform batch insertion
    if data:
        cursor.executemany(sql, data)
        cnxn.commit()
        print(f"Successfully inserted {len(data)} records.")
    else:
        print("No valid data to insert.")

except Exception as e:
    print(f"Error during batch insert: {e}")

finally:
    # Close the connection
    cursor.close()
    cnxn.close()
