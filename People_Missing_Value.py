# -*- coding: utf-8 -*-
"""
Script to handle missing VEHICLE_ID values and other missing values for specified attributes.
"""

# File paths
people_file_path = "/Users/shreya/Desktop/DSS/People.csv"
vehicle_file_path = "/Users/shreya/Desktop/DSS/Vehicles.csv"

# Output file paths
people_output_path = "/Users/shreya/Desktop/DSS/People_updated.csv"

# Default values for missing data
default_vehicle_id = "-1"
bac_result_default = "test not offered"
general_default = "UNKNOWN"

# Attributes to handle for missing values (except DAMAGE)
attributes_to_handle = [
    "CITY", "STATE", "SEX", "AGE", "SAFETY_EQUIPMENT",
    "AIRBAG_DEPLOYED", "EJECTION", "INJURY_CLASSIFICATION",
    "DRIVER_ACTION", "DRIVER_VISION", "PHYSICAL_CONDITION"
]

# Function to read and parse a CSV file into a list of rows
def read_csv(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    header = lines[0].strip().split(",")
    data = [line.strip().split(",") for line in lines[1:]]
    return header, data

# Function to write updated data back to a CSV file
def write_csv(file_path, header, data):
    with open(file_path, 'w') as file:
        file.write(",".join(header) + "\n")  # Write header
        for row in data:
            file.write(",".join(row) + "\n")  # Write rows

# Function to detect missing values
def is_missing(value):
    return value.strip() == "" or value.strip().lower() in ["na", "null", "n/a"]

# Read People.csv
people_header, people_data = read_csv(people_file_path)

# Read Vehicle.csv
vehicle_header, vehicle_data = read_csv(vehicle_file_path)

# Identify column indices for RD_NO and VEHICLE_ID
try:
    rd_no_index_people = people_header.index("RD_NO")
    vehicle_id_index_people = people_header.index("VEHICLE_ID")
except ValueError:
    print("RD_NO or VEHICLE_ID column not found in People.csv.")
    exit()

try:
    rd_no_index_vehicle = vehicle_header.index("RD_NO")
    vehicle_id_index_vehicle = vehicle_header.index("VEHICLE_ID")
except ValueError:
    print("RD_NO or VEHICLE_ID column not found in Vehicle.csv.")
    exit()

# Create dictionaries for quick lookup by RD_NO
vehicle_lookup = {row[rd_no_index_vehicle]: row[vehicle_id_index_vehicle] for row in vehicle_data}

# Update missing VEHICLE_ID in People.csv
for row in people_data:
    if is_missing(row[vehicle_id_index_people]):  # If missing VEHICLE_ID
        rd_no = row[rd_no_index_people]
        if rd_no in vehicle_lookup and not is_missing(vehicle_lookup[rd_no]):
            row[vehicle_id_index_people] = vehicle_lookup[rd_no]  # Fill VEHICLE_ID from Vehicle.csv
        else:
            row[vehicle_id_index_people] = default_vehicle_id  # Assign default value if no match

# Handle missing DAMAGE values specifically
try:
    damage_index = people_header.index("DAMAGE")
    for row in people_data:
        if is_missing(row[damage_index]):  # Only update if DAMAGE is missing
            row[damage_index] = general_default
except ValueError:
    print("DAMAGE column not found in People.csv.")

# Handle other attributes generically
for row in people_data:
    for attribute in attributes_to_handle:
        try:
            col_index = people_header.index(attribute)
            if is_missing(row[col_index]):  # Only update if the value is missing
                row[col_index] = general_default
        except ValueError:
            print(f"Attribute {attribute} not found in People.csv.")
            continue

# Handle BAC_RESULT specifically
try:
    bac_result_index = people_header.index("BAC_RESULT")
    for row in people_data:
        if is_missing(row[bac_result_index]):
            row[bac_result_index] = bac_result_default  # Set BAC_RESULT default value
except ValueError:
    print("BAC_RESULT column not found in People.csv.")

# Write updated People.csv
write_csv(people_output_path, people_header, people_data)

# Count remaining missing values
missing_counts = {attribute: 0 for attribute in people_header}

for row_index, row in enumerate(people_data):
    if len(row) != len(people_header):  # Check for length mismatch
        print(f"Row {row_index + 1} length mismatch: Expected {len(people_header)}, got {len(row)}. Skipping row.")
        continue  # Skip this row if mismatched
    
    for index, value in enumerate(row):
        if is_missing(value):  # Count missing values
            missing_counts[people_header[index]] += 1

print("Remaining missing values by attribute:")
print(missing_counts)



