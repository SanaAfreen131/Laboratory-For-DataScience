import csv

def load_dataset(file_path):
    """
    Load the dataset from a CSV file.
    """
    with open(file_path, mode="r") as file:
        reader = csv.DictReader(file)
        return [row for row in reader]

def save_dataset(data, file_path):
    """
    Save the processed dataset to a CSV file.
    """
    with open(file_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

def fill_report_type(record):
    """
    Fill the REPORT_TYPE column based on CRASH_TYPE.
    """
    if record["REPORT_TYPE"] is None or record["REPORT_TYPE"].strip() == "":
        if record["CRASH_TYPE"] == "INJURY AND / OR TOW DUE TO CRASH":
            return "At Scene"
        elif record["CRASH_TYPE"] == "NO INJURY / DRIVE AWAY":
            return "At Desk"
    return record["REPORT_TYPE"]

def fill_street_direction(record, direction_map):
    """
    Fill STREET_DIRECTION using a mapping from STREET_NAME.
    """
    if record["STREET_DIRECTION"] is None or record["STREET_DIRECTION"].strip() == "":
        if record["STREET_NAME"] in direction_map:
            return direction_map[record["STREET_NAME"]]
    return record["STREET_DIRECTION"]

def fill_lat_lon(record, lat_lon_map):
    """
    Fill LATITUDE and LONGITUDE using a predefined mapping.
    """
    if (not record["LATITUDE"] or record["LATITUDE"].strip() == "") or \
       (not record["LONGITUDE"] or record["LONGITUDE"].strip() == ""):
        # Use the mapping based on STREET_NAME and STREET_DIRECTION
        key = f"{record.get('STREET_NAME', '').strip()}_{record.get('STREET_DIRECTION', '').strip()}"
        if key in lat_lon_map:
            return lat_lon_map[key]
        else:
            # Assign default values if mapping is unavailable
            return "41.8781", "-87.6298"  # Default: Center of Chicago
    return record["LATITUDE"], record["LONGITUDE"]

def fill_location(record):
    """
    Fill LOCATION based on LATITUDE and LONGITUDE.
    """
    if record["LATITUDE"] and record["LONGITUDE"]:
        return f"{record['LATITUDE']},{record['LONGITUDE']}"
    return record["LOCATION"]

def fill_most_severe_injury(record):
    """
    Fill MOST_SEVERE_INJURY based on injury-related columns.
    """
    if record["MOST_SEVERE_INJURY"] is None or record["MOST_SEVERE_INJURY"].strip() == "":
        return "No Injury"  # Default value for missing
    return record["MOST_SEVERE_INJURY"]

def fill_street_name(record, street_map):
    """
    Fill STREET_NAME using predefined mappings or default values.
    """
    if record["STREET_NAME"] is None or record["STREET_NAME"].strip() == "":
        key = record.get("BEAT_OF_OCCURRENCE", "").strip()
        if key in street_map:
            return street_map[key]
        return "UNKNOWN STREET"  # Default placeholder
    return record["STREET_NAME"]

def fill_beat_of_occurrence(record, beat_map):
    """
    Fill BEAT_OF_OCCURRENCE using predefined mappings or default values.
    """
    if record["BEAT_OF_OCCURRENCE"] is None or record["BEAT_OF_OCCURRENCE"].strip() == "":
        key = record.get("STREET_NAME", "").strip()
        if key in beat_map:
            return beat_map[key]
        return "UNKNOWN"  # Default placeholder
    return record["BEAT_OF_OCCURRENCE"]

def process_dataset(dataset):
    """
    Process the dataset to handle missing values.
    """
    # Create mappings for STREET_DIRECTION and BEAT_OF_OCCURRENCE
    direction_map = {rec["STREET_NAME"]: rec["STREET_DIRECTION"] for rec in dataset if rec["STREET_NAME"] and rec["STREET_DIRECTION"]}
    lat_lon_map = {
        "S CHICAGO SKYWAY_OB": ("41.7241", "-87.5649"),
        "S DR MARTIN LUTHER KING JR_SD": ("41.8716", "-87.6175"),
        # Add more mappings as needed...
    }
    street_map = {
        "12345": "MAIN STREET",  # Replace with real mappings
        # Add more mappings as needed...
    }
    beat_map = {
        "MAIN STREET": "456",  # Replace with real mappings
        # Add more mappings as needed...
    }

    for record in dataset:
        # Fill REPORT_TYPE
        record["REPORT_TYPE"] = fill_report_type(record)

        # Fill STREET_DIRECTION
        record["STREET_DIRECTION"] = fill_street_direction(record, direction_map)

        # Fill LATITUDE and LONGITUDE
        lat, lon = fill_lat_lon(record, lat_lon_map)
        record["LATITUDE"], record["LONGITUDE"] = lat, lon

        # Fill LOCATION
        record["LOCATION"] = fill_location(record)

        # Fill MOST_SEVERE_INJURY
        record["MOST_SEVERE_INJURY"] = fill_most_severe_injury(record)

        # Fill STREET_NAME
        record["STREET_NAME"] = fill_street_name(record, street_map)

        # Fill BEAT_OF_OCCURRENCE
        record["BEAT_OF_OCCURRENCE"] = fill_beat_of_occurrence(record, beat_map)

    return dataset

if __name__ == "__main__":
    # Specify the file paths
    input_file = r'C:\Users\Lenovo\OneDrive\Desktop\Data Science\Decision support project\LDS24 - Data\crashes.csv'
    output_file = r'C:\Users\Lenovo\OneDrive\Desktop\Data Science\Decision support project\LDS24 - Data\cleaned_crashes.csv'

    # Load the dataset
    dataset = load_dataset(input_file)

    # Process the dataset
    processed_data = process_dataset(dataset)

    # Save the processed dataset
    save_dataset(processed_data, output_file)
    print(f"Processed data saved to {output_file}")
