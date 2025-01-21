# -*- coding: utf-8 -*-
"""
Created on Fri Nov 29 03:43:14 2024

@author:SANA AFREEN
"""

# Section 3
# ----------------- Splitting Pre-Processed Dataset into Dimensional Tables ------------

import csv

# Define the input file path
input_file = r'C:\DSS_LAB\Processed_Merged_Data.csv'

# Function to extract selected columns and write to a CSV file
def write_selected_columns(output_file, selected_columns):
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=selected_columns)
        writer.writeheader()
        
        for row in reader:
            filtered_row = {col: row[col] for col in selected_columns}
            writer.writerow(filtered_row)
    
    print(f"Selected columns have been written to {output_file}")

# ---- DIM_DATE ----
output_file = r'C:\DSS_LAB\Dim_Date.csv'
selected_columns = ['RD_NO', 'CRASH_DATE', 'DATE_POLICE_NOTIFIED', 'Date', 'Time', 'Day', 'Month', 'Year', 'Quarter', 'DayOfWeek', 'IsWeekend']
write_selected_columns(output_file, selected_columns)

# ---- DIM_BEAT ----
output_file = r'C:\DSS_LAB\Dim_Beat.csv'
selected_columns = ['RD_NO', 'BEAT_OF_OCCURRENCE']
write_selected_columns(output_file, selected_columns)

# ---- DIM_CAUSE ----
output_file = r'C:\DSS_LAB\Dim_Cause.csv'
selected_columns = ['RD_NO', 'PRIM_CONTRIBUTORY_CAUSE', 'SEC_CONTRIBUTORY_CAUSE', 'DRIVER_ACTION', 'DRIVER_VISION']
write_selected_columns(output_file, selected_columns)

# ---- DIM_VEHICLE ----
output_file = r'C:\DSS_LAB\Dim_Vehicle.csv'
selected_columns = ['RD_NO', 'VEHICLE_ID', 'CRASH_UNIT_ID', 'VEHICLE_TYPE', 'MAKE', 'MODEL', 'VEHICLE_YEAR', 'LIC_PLATE_STATE', 'VEHICLE_DEFECT', 'VEHICLE_USE', 'OCCUPANT_CNT', 'UNIT_TYPE', 'TRAVEL_DIRECTION', 'UNIT_NO', 'MANEUVER']
write_selected_columns(output_file, selected_columns)

# ---- DIM_CLIENT ----
output_file = r'C:\DSS_LAB\Dim_Client.csv'
selected_columns = ['RD_NO', 'PERSON_ID', 'PERSON_TYPE', 'AGE', 'SEX', 'CITY', 'STATE', 'INJURY_CLASSIFICATION', 'PHYSICAL_CONDITION', 'SAFETY_EQUIPMENT', 'EJECTION', 'AIRBAG_DEPLOYED', 'BAC_RESULT']
write_selected_columns(output_file, selected_columns)

# ---- DIM_CRASH ----
output_file = r'C:\DSS_LAB\Dim_Crash.csv'
selected_columns = ['RD_NO', 'POSTED_SPEED_LIMIT', 'TRAFFIC_CONTROL_DEVICE', 'DEVICE_CONDITION', 'WEATHER_CONDITION', 'LIGHTING_CONDITION', 'ROADWAY_SURFACE_COND', 'FIRST_CRASH_TYPE', 'TRAFFICWAY_TYPE', 'ALIGNMENT', 'STREET_NO', 'STREET_NAME', 'STREET_DIRECTION', 'CRASH_TYPE', 'LATITUDE', 'LONGITUDE', 'LOCATION', 'ROAD_DEFECT', 'FIRST_CONTACT_POINT', 'REPORT_TYPE']
write_selected_columns(output_file, selected_columns)

# ---- FACT_CRASH ----
output_file = r'C:\DSS_LAB\Fact_Crash.csv'
selected_columns = ['RD_NO', 'INJURIES_TOTAL', 'INJURIES_FATAL', 'INJURIES_INCAPACITATING', 'INJURIES_REPORTED_NOT_EVIDENT', 'NUM_UNITS', 'DAMAGE', 'DAMAGE_CATEGORY', 'INJURIES_NO_INDICATION', 'INJURIES_UNKNOWN', 'INJURIES_NON_INCAPACITATING', 'MOST_SEVERE_INJURY']
write_selected_columns(output_file, selected_columns)