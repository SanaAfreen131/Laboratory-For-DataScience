# -*- coding: utf-8 -*-
"""
Created on Fri Nov 29 02:37:24 2024

@author: SANA AFREEN
"""

# Section 2
# ----------------- Extracting Date and Time, Creating New Columns ------------ 
import csv
from datetime import datetime

# File paths
merged_file = r'C:\DSS_LAB\Merged_Data.csv'
processed_file = r'C:\DSS_LAB\Processed_Merged_Data.csv'

# Process merged data to add date and time columns
with open(merged_file, 'r') as merged, open(processed_file, 'w', newline='') as processed:
    reader = csv.DictReader(merged)
    fieldnames = reader.fieldnames + ['Date', 'Time', 'Day', 'Month', 'Year', 'Quarter', 'DayOfWeek', 'IsWeekend']
    writer = csv.DictWriter(processed, fieldnames=fieldnames)
    
    writer.writeheader()
    
    for row in reader:
        crash_date_str = row.get('CRASH_DATE', '')  # Retrieve CRASH_DATE
        if crash_date_str:
            try:
                # Parse the date and time
                crash_date = datetime.strptime(crash_date_str, '%m/%d/%Y %I:%M:%S %p')
                
                # Create new columns
                row['Date'] = crash_date.strftime('%Y-%m-%d')
                row['Time'] = crash_date.strftime('%H:%M:%S')
                row['Day'] = crash_date.day
                row['Month'] = crash_date.month
                row['Year'] = crash_date.year
                row['Quarter'] = (crash_date.month - 1) // 3 + 1
                row['DayOfWeek'] = crash_date.strftime('%A')
                row['IsWeekend'] = 1 if crash_date.weekday() >= 5 else 0
            except ValueError:
                # Handle invalid date format
                row['Date'] = row['Time'] = ''
                row['Day'] = row['Month'] = row['Year'] = ''
                row['Quarter'] = row['DayOfWeek'] = row['IsWeekend'] = ''
        else:
            # Handle missing CRASH_DATE
            row['Date'] = row['Time'] = ''
            row['Day'] = row['Month'] = row['Year'] = ''
            row['Quarter'] = row['DayOfWeek'] = row['IsWeekend'] = ''
        
        writer.writerow(row)

print(f"Processed data with separate date and time columns has been saved to {processed_file}.")
