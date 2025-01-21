
# Section 1
# ----------------- Merging All 3 Datasets and Creating Merged Output File ------------
import csv

# Define file paths
crashes_file = r'C:\DSS_LAB\Crashes.csv'
people_file = r'C:\DSS_LAB\People.csv'
vehicles_file = r'C:\DSS_LAB\Vehicles.csv'
merged_file = r'C:\DSS_LAB\Merged_Data.csv'

# Merge datasets into a single file
with open(crashes_file, 'r') as crashes, open(people_file, 'r') as people, open(vehicles_file, 'r') as vehicles, open(merged_file, 'w', newline='') as merged:
    crashes_reader = csv.DictReader(crashes)
    people_reader = csv.DictReader(people)
    vehicles_reader = csv.DictReader(vehicles)
    
    # Get headers from all datasets
    crashes_headers = crashes_reader.fieldnames
    people_headers = people_reader.fieldnames
    vehicles_headers = vehicles_reader.fieldnames
    
    # Combine headers, ensuring no duplicates
    combined_headers = list(set(crashes_headers + people_headers + vehicles_headers))
    
    # Write merged header to the output file
    merged_writer = csv.DictWriter(merged, fieldnames=combined_headers)
    merged_writer.writeheader()
    
    # Create lookup dictionaries for Vehicles and People datasets
    vehicle_data = {}
    for vehicle_row in vehicles_reader:
        key = (vehicle_row['RD_NO'], vehicle_row['Vehicle_ID'])
        vehicle_data[key] = vehicle_row  # Group by RD_NO and VehicleID
    
    people_data = {}
    for person_row in people_reader:
        key = (person_row['RD_NO'], person_row['Vehicle_ID'])
        people_data.setdefault(key, []).append(person_row)  # Group by RD_NO and VehicleID
    
    # Merge Vehicles and People
    merged_vehicle_people = {}
    for key, vehicle_row in vehicle_data.items():
        rd_no, vehicle_id = key
        matching_people = people_data.get(key, [])
        for person_row in matching_people:
            merged_vehicle_people.setdefault(rd_no, []).append({**vehicle_row, **person_row})
    
    # Merge Crashes with Vehicle-People Merged Data
    for crash_row in crashes_reader:
        rd_no = crash_row['RD_NO']
        merged_data_for_crash = merged_vehicle_people.get(rd_no, [])
        
        # If no merged vehicle-people data, write crash data alone
        if not merged_data_for_crash:
            merged_writer.writerow(crash_row)
        else:
            # Write each merged vehicle-people row combined with crash data
            for vehicle_people_row in merged_data_for_crash:
                merged_row = {**crash_row, **vehicle_people_row}
                merged_writer.writerow(merged_row)
