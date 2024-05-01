'''The following code is an implementation of a data pipeline that extracts, transforms, and loads data'''

"""testing by commenting this line"""
import requests
import json
import os
import csv
import boto3
import openpyxl
from io import StringIO


def load_configuration():
    # Load configuration from config.json
    with open("/app/config.json") as config_file:
        return json.load(config_file)

def get_current_previous_years_data(config):
    #retrieves the weather data for a specific city and year range.
    combined_data = []
    base_url = config["base_url"]
    station_id = config["station_id"]
    timeframe = config["timeframe"]
    submit = config["submit"]
    input_year = config["input_year"]
    
    # Iterate over a range of years starting from input_year - 2
    for year in range(input_year - 2, input_year + 1):
        url = f"{base_url}?format=csv&stationID={station_id}&Year={year}&timeframe={timeframe}&submit={submit}"
        response = requests.get(url)

        if response.status_code == 200:
            # Parse the CSV data
            reader = csv.reader(response.content.decode('utf-8-sig').splitlines())
            data = list(reader)

            # Filter out future dates and rows with missing temperature values
            year_column_name = "Year"
            temperature_column_name = "Max Temp (°C)"

            # Find the indices of the year and temperature columns
            year_column_index = data[0].index(year_column_name)
            temperature_column_index = data[0].index(temperature_column_name)

            # Append the filtered data to the combined list
            for row in data[1:]:
                if row[year_column_index] == str(year) and row[temperature_column_index]:
                    combined_data.append(row)

            print(f"Data downloaded for year {year}.")
        else:
            print(f"Failed to download data for year {year}.")
    
        # Save the combined data to a string variable
    output_string = StringIO()
    writer = csv.writer(output_string)
    writer.writerows([data[0]] + combined_data)  # Include the header row once

    output_string.seek(0)  # Move the cursor to the beginning of the string

    # Read the CSV data from the variable
    weather_data = csv.DictReader(output_string)
    return weather_data 


def get_station_data():
    # Load station data
    with open("/app/station_data.csv", "r") as station_file:
        station_reader = csv.DictReader(station_file)
        station_data = list(station_reader)
        return station_data


def join_station_weather_data(weather_data,station_data):
    # Create a list to store the joined data with selected columns
    joined_data = []

    # Select the columns you're interested in from weather data
    weather_selected_columns = ["Longitude (x)", "Latitude (y)", "Station Name", "Climate ID", "Date/Time",
                                "Year", "Month", "Day", "Data Quality", "Max Temp (°C)", "Max Temp Flag",
                                "Min Temp (°C)", "Min Temp Flag", "Mean Temp (°C)", "Mean Temp Flag",
                                "Heat Deg Days (°C)", "Heat Deg Days Flag", "Cool Deg Days (°C)",
                                "Cool Deg Days Flag", "Total Rain (mm)", "Total Rain Flag",
                                "Total Snow (cm)", "Total Snow Flag", "Total Precip (mm)"]

    # Select the columns you're interested in from station data
    station_selected_columns = ["Province", "Station ID", "WMO ID", "Elevation (m)",
                                "First Year", "Last Year", "HLY First Year", "HLY Last Year",
                                "DLY First Year", "DLY Last Year", "MLY First Year", "MLY Last Year"]

    # Perform the join based on Climate ID
    for weather_row in weather_data:
        climate_id = weather_row["Climate ID"]
        for station_row in station_data:
            if station_row["Climate ID"] == climate_id:
                joined_row = {column: weather_row[column] for column in weather_selected_columns}
                joined_row.update({column: station_row[column] for column in station_selected_columns})
                joined_data.append(joined_row)
                break

    # Write the joined data to a new CSV file
    output_filename = "joined_data.csv"
    fieldnames = weather_selected_columns + station_selected_columns
    with open(output_filename, "w", newline='', encoding='utf-8-sig') as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(joined_data)

    # Load the joined data
    with open(output_filename, "r") as joined_data_file:
        joined_data = csv.DictReader(joined_data_file)
        joined_data = list(joined_data)
    
    return joined_data


def generate_partition_data(joined_data):
    # Create a dictionary to store the partitioned data
    partitioned_data = {}

    # Read the joined data and partition it by city and year
    for row in joined_data:
        city = row['Station Name']  # Update the column name to 'Station Name'
        year = row['Year']
        if city not in partitioned_data:
            partitioned_data[city] = {}
        if year not in partitioned_data[city]:
            partitioned_data[city][year] = []
        partitioned_data[city][year].append(row)
    return partitioned_data

def upload_data_to_s3(partitioned_data, fieldnames,config):
    access_key = config["aws_access_key_id"]
    secret_key = config["aws_secret_access_key"]
    region = config["region"]
    bucket_name = config["bucket_name"]
    # Upload the partitioned data to S3
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)
    for city, years in partitioned_data.items():
        for year, data in years.items():
            output_directory = f"{city}/{year}"
            os.makedirs(output_directory, exist_ok=True)  # Create the directory if it doesn't exist
            output_filename = f"{output_directory}/{year}.csv"
            with open(output_filename, 'w', newline='') as output_file:
                writer = csv.DictWriter(output_file, fieldnames)
                writer.writeheader()
                writer.writerows(data)
            s3.upload_file(output_filename, bucket_name, output_filename)

    print("Data uploaded to S3 bucket.")


def generate_excel_file(partitioned_data,joined_data):
    # Generate an Excel file with data divided into years
    excel_filename = "Final_weather_report.xlsx"
    workbook = openpyxl.Workbook()

    # Create a separate sheet for each year
    for city, years in partitioned_data.items():
        for year, data in years.items():
            sheet = workbook.create_sheet(title=year)  # Use year as the sheet title

            # Write the header row
            header_row = list(joined_data[0].keys())
            sheet.append(header_row)

            # Write the data rows
            for row in data:
                sheet.append(list(row.values()))

    # Remove the default sheet created by openpyxl
    workbook.remove(workbook["Sheet"])

    # Save the Excel file to local storage
    workbook.save(excel_filename)

    print(f"Excel file '{excel_filename}' is generated.")


def main():
    config = load_configuration()
    weather_data = get_current_previous_years_data(config)
    station_data = get_station_data()
    joined_data= join_station_weather_data(weather_data,station_data)
    fieldnames = list(joined_data[0].keys())
    partitioned_data=generate_partition_data(joined_data)
    upload_data_to_s3(partitioned_data, fieldnames,config)
    generate_excel_file(partitioned_data, joined_data)


if __name__ == "__main__":
    main()