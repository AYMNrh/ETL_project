# ETL_project


file: 
Invoke-WebRequest -Uri "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip" -OutFile ".\ETL_project\project\source.zip"


Pandas: python3.11 -m pip install pandas

TODO:
https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip 

Create a folder data_source and use the terminal shell to change the current directory to \home\project\data_source. Create a file etl_practice.py in this folder.

Download and unzip the data available in the link shared above.

The data available has four headers: 'car_model', 'year_of_manufacture', 'price', 'fuel'. Implement the extraction process for the CSV, JSON, and XML files.

Transform the values under the 'price' header such that they are rounded to 2 decimal places.

Implement the loading function for the transformed data to a target file, transformed_data.csv.

Implement the logging function for the entire process and save it in log_file.txt.

Test the implemented functions and log the events.