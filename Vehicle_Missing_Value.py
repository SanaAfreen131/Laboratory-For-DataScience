import csv
from collections import Counter

# Function to handle missing values based on the given rules
def handle_missing_values(input_file, output_file):
    # Read the CSV file
    with open(input_file, mode='r') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        rows = list(reader)

    # Initialize dictionaries to store associations between MAKE, MODEL, and VEHICLE_YEAR
    model_to_make = {}
    make_to_model_year = {}
    model_to_year = {}

    # Populate the dictionaries with relevant associations
    for row in rows:
        make = row['MAKE']
        model = row['MODEL']
        vehicle_year = row['VEHICLE_YEAR']
        
        # Make to Model-Year associations
        if make and model and vehicle_year:
            if make not in make_to_model_year:
                make_to_model_year[make] = {}
            if model not in make_to_model_year[make]:
                make_to_model_year[make][model] = []
            make_to_model_year[make][model].append(vehicle_year)

        # Model to Make associations
        if model and make:
            if model not in model_to_make:
                model_to_make[model] = []
            model_to_make[model].append(make)
        
        # Model to Vehicle Year associations
        if model and vehicle_year:
            if model not in model_to_year:
                model_to_year[model] = []
            model_to_year[model].append(vehicle_year)

    # Function to get the most frequent item from a list
    def most_frequent(lst):
        if lst:
            return Counter(lst).most_common(1)[0][0]
        return 'UNKNOWN'

    # Process each row to fill missing values
    for row in rows:
        # Handle Make (If MAKE is missing, refer to MODEL)
        if row['MAKE'] == '' or row['MAKE'] is None:
            model = row['MODEL']
            if model in model_to_make:
                row['MAKE'] = most_frequent(model_to_make[model])
            else:
                row['MAKE'] = 'UNKNOWN'

        # Handle Model (If MODEL is missing, refer to MAKE and VEHICLE_YEAR)
        if row['MODEL'] == '' or row['MODEL'] is None:
            make = row['MAKE']
            vehicle_year = row['VEHICLE_YEAR']
            if make in make_to_model_year and vehicle_year in make_to_model_year[make]:
                row['MODEL'] = most_frequent(make_to_model_year[make][vehicle_year])
            else:
                row['MODEL'] = 'UNKNOWN'

        # Handle Vehicle Year (If VEHICLE_YEAR is missing, refer to MODEL)
        if row['VEHICLE_YEAR'] == '' or row['VEHICLE_YEAR'] is None:
            model = row['MODEL']
            if model in model_to_year:
                row['VEHICLE_YEAR'] = most_frequent(model_to_year[model])
            else:
                row['VEHICLE_YEAR'] = 'UNKNOWN'

        # Handle LIC Plate State (Fill with 'UNKNOWN' if missing)
        if row['LIC_PLATE_STATE'] == '' or row['LIC_PLATE_STATE'] is None:
            row['LIC_PLATE_STATE'] = 'UNKNOWN'

        # Handle UNIT_TYPE (Fill with 'UNKNOWN' if missing)
        if row['UNIT_TYPE'] == '' or row['UNIT_TYPE'] is None:
            row['UNIT_TYPE'] = 'UNKNOWN'

        # Handle Vehicle ID (Fill with 'UNKNOWN' if missing)
        if row['VEHICLE_ID'] == '' or row['VEHICLE_ID'] is None:
            row['VEHICLE_ID'] = 'UNKNOWN'

        # Handle Vehicle Defect (Fill with 'UNKNOWN' if missing)
        if row['VEHICLE_DEFECT'] == '' or row['VEHICLE_DEFECT'] is None:
            row['VEHICLE_DEFECT'] = 'UNKNOWN'

        # Handle Vehicle Type (Fill with 'UNKNOWN' if missing)
        if row['VEHICLE_TYPE'] == '' or row['VEHICLE_TYPE'] is None:
            row['VEHICLE_TYPE'] = 'UNKNOWN'

        # Handle Vehicle Use (Fill with 'UNKNOWN' if missing)
        if row['VEHICLE_USE'] == '' or row['VEHICLE_USE'] is None:
            row['VEHICLE_USE'] = 'UNKNOWN'

        # Handle Travel Direction (Fill with 'UNKNOWN' if missing)
        if row['TRAVEL_DIRECTION'] == '' or row['TRAVEL_DIRECTION'] is None:
            row['TRAVEL_DIRECTION'] = 'UNKNOWN'

        # Handle Maneuver (Fill with 'UNKNOWN' if missing)
        if row['MANEUVER'] == '' or row['MANEUVER'] is None:
            row['MANEUVER'] = 'UNKNOWN'

        # Handle Number of Occupants (Fill with 'UNKNOWN' if missing)
        if row['OCCUPANT_CNT'] == '' or row['OCCUPANT_CNT'] is None:
            row['OCCUPANT_CNT'] = 'UNKNOWN'

        # Handle First Contact Point (Fill with 'UNKNOWN' if missing)
        if row['FIRST_CONTACT_POINT'] == '' or row['FIRST_CONTACT_POINT'] is None:
            row['FIRST_CONTACT_POINT'] = 'UNKNOWN'

    # Write the processed data to a new CSV file
    with open(output_file, mode='w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

# Usage
input_file = r'C:\Users\Lenovo\OneDrive\Desktop\Data Science\Decision support project\LDS24 - Data\vehicles.csv'  
output_file = r'C:\Users\Lenovo\OneDrive\Desktop\Data Science\Decision support project\LDS24 - Data\cleaned_vehicles.csv'  
handle_missing_values(input_file, output_file)

print(f"Processed file saved as {output_file}")
