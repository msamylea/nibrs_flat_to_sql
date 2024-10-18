import pandas as pd
import numpy as np
import multiprocessing as mp
import sqlite3
from itertools import islice
import json

def safe_substr(s, start, end):
    return s[start:end] if start < len(s) else ''

def parse_record(line):
    record_type = safe_substr(line, 0, 2)

    if record_type == 'BH':
        return parse_bh_record(line)
    elif record_type == '01':
        return parse_01_record(line)
    elif record_type == '02':
        return parse_02_record(line)
    elif record_type == '03':
        return parse_03_record(line)
    elif record_type == '04':
        return parse_04_record(line)
    elif record_type == '05':
        return parse_05_record(line)
    elif record_type == '06':
        return parse_06_record(line)
    else:
        return None

def parse_bh_record(line):
    return {
        'ORI': safe_substr(line, 16, 25),
        'City': safe_substr(line, 40, 65).strip(),
        'State': safe_substr(line, 65, 67),
        'Population': safe_substr(line, 67, 75),
        'Year': safe_substr(line, 8, 12)
    }

def parse_01_record(line):
    return {
        'ORI': safe_substr(line, 16, 25),
        'Incident_Number': safe_substr(line, 25, 37),
        'Incident_Date': safe_substr(line, 37, 45),
        'Report_Date_Indicator': safe_substr(line, 45, 46),
        'Incident_Hour': safe_substr(line, 46, 48),
        'Cleared_Exceptionally': safe_substr(line, 48, 49),
        'Exceptional_Clearance_Date': safe_substr(line, 49, 57)
    }

def parse_02_record(line):
    return {
        'ORI': safe_substr(line, 16, 25),
        'Incident_Number': safe_substr(line, 25, 37),
        'UCR_Offense_Code': safe_substr(line, 37, 40),
        'Attempted_Completed': safe_substr(line, 40, 41),
        'Offender_Suspected_of_Using': [safe_substr(line, 41, 42), safe_substr(line, 42, 43), safe_substr(line, 43, 44)],
        'Location_Type': safe_substr(line, 44, 46),
        'Number_of_Premises_Entered': safe_substr(line, 46, 48),
        'Method_of_Entry': safe_substr(line, 48, 49),
        'Type_Criminal_Activity': [safe_substr(line, 49, 51), safe_substr(line, 51, 53), safe_substr(line, 53, 55)],
        'Weapon_Force_Involved': [safe_substr(line, 55, 57), safe_substr(line, 57, 59), safe_substr(line, 59, 61)],
        'Bias_Motivation': safe_substr(line, 61, 63)
    }

def parse_03_record(line):
    return {
        'ORI': safe_substr(line, 16, 25),
        'Incident_Number': safe_substr(line, 25, 37),
        'Type_Property_Loss': safe_substr(line, 37, 38),
        'Property_Description': [safe_substr(line, 38+i*21, 40+i*21) for i in range(10)],
        'Property_Value': [safe_substr(line, 40+i*21, 48+i*21) for i in range(10)],
        'Date_Recovered': [safe_substr(line, 48+i*21, 56+i*21) for i in range(10)],
        'Number_Stolen_Vehicles': safe_substr(line, 228, 230),
        'Number_Recovered_Vehicles': safe_substr(line, 230, 232)
    }

def parse_04_record(line):
    return {
        'ORI': safe_substr(line, 16, 25),
        'Incident_Number': safe_substr(line, 25, 37),
        'Victim_Sequence_Number': safe_substr(line, 37, 40),
        'Victim_Connected_UCR_Offense_Code': [safe_substr(line, 40+i*3, 43+i*3) for i in range(10)],
        'Type_of_Victim': safe_substr(line, 70, 71),
        'Age_of_Victim': safe_substr(line, 71, 75),
        'Sex_of_Victim': safe_substr(line, 75, 76),
        'Race_of_Victim': safe_substr(line, 76, 77),
        'Ethnicity_of_Victim': safe_substr(line, 77, 78),
        'Resident_Status_of_Victim': safe_substr(line, 78, 79),
        'Aggravated_Assault_Homicide_Circumstances': [safe_substr(line, 79, 81), safe_substr(line, 81, 83)],
        'Additional_Justifiable_Homicide_Circumstances': safe_substr(line, 83, 84),
        'Type_Injury': [safe_substr(line, 84, 85), safe_substr(line, 85, 86), safe_substr(line, 86, 87), safe_substr(line, 87, 88), safe_substr(line, 88, 89)]
    }

def parse_05_record(line):
    return {
        'ORI': safe_substr(line, 16, 25),
        'Incident_Number': safe_substr(line, 25, 37),
        'Offender_Sequence_Number': safe_substr(line, 37, 39),
        'Age_of_Offender': safe_substr(line, 39, 43),
        'Sex_of_Offender': safe_substr(line, 43, 44),
        'Race_of_Offender': safe_substr(line, 44, 45)
    }

def parse_06_record(line):
    return {
        'ORI': safe_substr(line, 16, 25),
        'Incident_Number': safe_substr(line, 25, 37),
        'Arrestee_Sequence_Number': safe_substr(line, 37, 39),
        'Arrest_Transaction_Number': safe_substr(line, 39, 51),
        'Arrest_Date': safe_substr(line, 51, 59),
        'Type_of_Arrest': safe_substr(line, 59, 60),
        'Multiple_Arrestee_Segments_Indicator': safe_substr(line, 60, 61),
        'UCR_Arrest_Offense_Code': safe_substr(line, 61, 64),
        'Arrestee_Was_Armed_With': [safe_substr(line, 64, 66), safe_substr(line, 66, 68)],
        'Age_of_Arrestee': safe_substr(line, 70, 74),
        'Sex_of_Arrestee': safe_substr(line, 74, 75),
        'Race_of_Arrestee': safe_substr(line, 75, 76),
        'Ethnicity_of_Arrestee': safe_substr(line, 76, 77),
        'Resident_Status_of_Arrestee': safe_substr(line, 77, 78),
        'Disposition_of_Arrestee_Under_18': safe_substr(line, 78, 79)
    }

def process_chunk(chunk):
    records = {
        'BH': [], '01': [], '02': [], '03': [], '04': [], '05': [], '06': []
    }

    for line in chunk:
        try:
            record_type = line[:2]
            parsed_record = parse_record(line)
            if parsed_record:
                records[record_type].append(parsed_record)
        except Exception as e:
            print(f"Error processing line: {line}")
            print(f"Error message: {str(e)}")
            continue

    return {k: pd.DataFrame(v) for k, v in records.items() if v}

def list_to_json(item):
    if isinstance(item, list):
        return json.dumps(item)
    return item

def save_to_sqlite(df_dict, conn):
    for table_name, df in df_dict.items():
        # Convert list columns to JSON strings
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].apply(list_to_json)
        
        df.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"Saved {len(df)} rows to table {table_name}")

if __name__ == '__main__':
    file_path = "C:/Users/msamy/Downloads/2023_NIBRS_NATIONAL_MASTER_FILE.txt"
    chunk_size = 100000  # Adjust based on your system's memory
    
    conn = sqlite3.connect('nibrs_data.db')
    
    pool = mp.Pool(mp.cpu_count())
    
    total_lines_processed = 0
    
    with open(file_path, 'r') as f:
        while True:
            chunk = list(islice(f, chunk_size))
            if not chunk:
                break
            try:
                df_dict = pool.apply(process_chunk, (chunk,))
                for table, df in df_dict.items():
                    print(f"Processing table {table} with {len(df)} rows")
                save_to_sqlite(df_dict, conn)
                total_lines_processed += len(chunk)
                print(f"Processed {total_lines_processed} lines total")
            except Exception as e:
                print(f"Error processing chunk: {str(e)}")
                continue
    
    pool.close()
    pool.join()
    conn.close()
    
    print("Processing complete. Data saved to SQLite database.")

    # Verify the data was saved
    conn = sqlite3.connect('nibrs_data.db')
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    print("\nTables in the database:")
    for table in tables:
        print(table[0])

    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM '{table[0]}'")
        count = cursor.fetchone()[0]
        print(f"Number of rows in {table[0]}: {count}")

    conn.close()