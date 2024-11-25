# Importing the required libraries
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime


log_file = "log_file.txt" # Log file to store the logs
target_file = "transformed_data.csv" # Target file to store the transformed data


# 1 - Extracting the data

# extrcat_from_csv() function to extract data from csv files
# extract_from_json() function to extract data from json files
# extract_from_xml() function to extract data from xml files


def extract_from_csv(file):
    df = pd.read_csv(file)
    return df

def extract_from_json(file):
    df = pd.read_json(file, lines=True)
    return df

def extract_from_xml(file):
    df = pd.DataFrame(columns=["name","height","weight"])
    tree = ET.parse(file)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = person.find("height").text
        weight = person.find("weight").text
        df = pd.concat([df, pd.DataFrame([[name, height, weight]], columns=["name","height","weight"])]
                       , ignore_index=True)
    
    return df


def extract():
    extracted_data =  pd.DataFrame(columns=["name","height","weight"]) # Dataframe to store the extracted data

    # processing the csv files
    for csvfile in glob.glob("*.csv"):
        extracted_data = pd.concat([extracted_data, extract_from_csv(csvfile)], ignore_index=True)

    # processing the json files
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, extract_from_json(jsonfile)], ignore_index=True)

    # processing the xml files
    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, extract_from_xml(xmlfile)], ignore_index=True)

    return extracted_data

# 2 - Transforming the data

# transform height to cm from inches and weight to kg from pounds

def transform(data):
    '''convert height from inches to cm and weight from pounds to kg
    1 inch = 0.0254 meters and 1 pound = 0.453592 kg'''

    # Convert height and weight to numeric, forcing errors to NaN
    data["height"] = pd.to_numeric(data["height"], errors='coerce')
    data["weight"] = pd.to_numeric(data["weight"], errors='coerce')

    # Perform the conversion
    data["height"] = round(data["height"] * 0.0254, 2)
    data["weight"] = round(data["weight"] * 0.453592, 2)

    return data

# 3 - Loading the data

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)


# 4 - Logging the data

def logging(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Timestamp format
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')
    

# 5 - Running the ETL process

logging("ETL Job Started") # Logging the start of the ETL process

# Log the start of the ETL process
logging("Extract phase Started")
extracted_data = extract()

# Log the completion of the extract phase
logging("Extract phase Ended")


# Log the start of the transform phase
logging("Transform phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data: \n", transformed_data)

# Log the completion of the transform phase
logging("Transform phase Ended")

# Log the start of the load phase
logging("Load phase Started")
load_data(target_file, transformed_data)

# Log the completion of the load phase
logging("Load phase Ended")

# Log the completion of the ETL process
logging("ETL Job Ended")