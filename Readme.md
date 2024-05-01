
# WAVE-ASSIGNMENT Data Pipeline

This readme file provides a detailed overview of the data pipeline for the WAVE-ASSIGNMENT. The data pipeline performs extraction, transformation, and loading of data based on the provided requirements and packaging it in a docker file.


## Structure
The project folder "ASSIGNMENT" contains the following files and directories:

- `src/`: This directory contains the source code for the data pipeline.
	-  `config.json`: Configuration file containing settings for the data pipeline.
	-  `main.py`: The main script that executes the data pipeline tasks.
	-  `station_data.csv`: The Station Inventory file used in the data pipeline.
	-  `dockerfile`: Docker file for packaging the data pipeline.
	-  `requirements.txt`: File listing the required Python libraries for the data pipeline.
	   
## Prerequisites
- [Python 3.8+](https://docs.python.org/3.8/)
- Dependencies: `requests`, `boto3`, `openpyxl`
- [Station Inventory File](https://drive.google.com/drive/folders/1WJCDEU34c60IfOnG4rv5EPZ4IhhW9vZH)
- [Weather Data URL](https://climate.weather.gc.ca/climate_data/bulk_data_e.html)
- [AWS](https://aws.amazon.com/) S3 bucket and valid credentials (access key, secret key)
- [Docker](https://www.docker.com/)



## _Exercise 1 - Extraction, Transformation, Load_

## Code Breakdown

 - **Code Execution:**
		 The `main()` function serves as the entry point for the script execution and we can give `Year` and `city` as input variables.


 - **Data Retrieval:**
		 We have the data sources  `Station Inventory EN.csv` and weather data source URL given in the prerequisite. The `load_configuration()` function reads the configuration from a JSON file named "config.json" and returns the loaded data. The `get_current_previous_years_data(config)` function retrieves weather data for Toronto, Ontario, for the last 3 years from the input year and filtered data is stored in the `combined_data` list then it back as a CSV using the `csv.DictReader` object.


 - **Cleanup & Merge:**
		 The `get_station_data()` function reads station data from a CSV file named "station_data.csv" and returns the loaded data. The `join_station_weather_data(weather_data, station_data)` function performs a join operation between the weather data and station data which selects specific columns from each dataset to include in the joined data. Here we specify column names for both weather_data and station_data that need to be merged, this iterates over each weather row and matches it with a corresponding station row based on the "Climate ID" column where this joined data is stored in the "joined_data" list and then written to a new CSV file named "joined_data.csv"

 -  **Upload to an S3 bucket:**
		  The `upload_data_to_s3(partitioned_data, fieldnames, config)` function uploads the partitioned data to an S3 bucket by provided AWS credentials from the configuration to authenticate with the S3 service using the `boto3` library where it iterates over the partitioned data, creating a directory structure for each city and year combination and writes the data for each city and year to a CSV file. Finally, it uploads each CSV file to the specified S3 bucket using the `s3.upload_file()` method.
		 

		The below are the directory and files paths in S3 which contains data:
		

	 - [Amazon s3 > Buckets/wave-assignment](https://photos.app.goo.gl/GteWvjFMQ3a5o7KT9)
	 - [Amazon s3 > Buckets/wave-assignment/Toronto/](https://photos.app.goo.gl/k9MMqQ8Dpuvj1z5PA)
	 - [Amazon s3 > Buckets/wave-assignment/Toronto/1898](https://photos.app.goo.gl/adzGLvg69x6cNYvC7)

  
  5. **Merge:**
		The `generate_partition_data(joined_data)` function partitions the joined data into a dictionary structure. It creates a dictionary to store the partitioned data, where each city is a key and each year is a sub-dictionary and iterates over the joined data, adding each row to the appropriate city and year in the dictionary where partitioned data directory is returned.

		The `generate_excel_file(partitioned_data, joined_data)` function generates an Excel file with the partitioned data by creating a new Excel workbook  and separate sheet for each year within each city, it also writes the header row and data rows from the partitioned data to the corresponding sheets using the `openpyxl` library and workbook is saved as an Excel file named "[Final_weather_report.xlsx](https://docs.google.com/spreadsheets/d/1_iKvu00sGX2QjZSQCuxOBSrnGjDOeJxx/edit?usp=drive_link&ouid=100690193010079436622&rtpof=true&sd=true)".
		

  6. **Query:**
			_AWS  Athena_ is a serverless query service provided by AWS that allows you to analyze and query data directly in S3 using SQL-like syntax.

	**_setting up AWS athena:_**
			- Create an Athena database and define the table schema that corresponds to your data structure.
			- Define the table schema using CREATE TABLE statements or by using AWS Glue Crawler to automatically infer the schema.
			- Now open and Click on "Query editor" to open the Athena Query Editor and write to execute SQL queries.
			- The below are SQL queries that meets the query requirements:
				

#### Query to calculate maximum and minimum temperature for a specific year
SELECT
  MAX(`Max Temp (°C)`) AS max_temp,
  MIN(`Min Temp (°C)`) AS min_temp
FROM
  your_table_name
WHERE
  Year = <input_year>;


#### Query Percentage difference between the avg temperature for the year versus the avg of the past 2 years
SELECT
    (avg_temp_year - avg_temp_past_two_years) / avg_temp_past_two_years * 100 AS percentage_difference
FROM
    (
     SELECT
         AVG(CASE WHEN year = <input_year> THEN temperature END) AS avg_temp_year,
        AVG(CASE WHEN year BETWEEN <input_year> - 2 AND <input_year> - 1 THEN temperature END) AS avg_temp_past_two_years
    FROM
        weather_data
    WHERE
        city = '<input_city>'
    ) AS subquery;


#### Query to show difference between the average temperature per month for year
    
SELECT month, AVG(temperature) - previous_year_avg AS temperature_difference
FROM
    (
     SELECT
         MONTH(date) AS month,
        AVG(temperature) AS previous_year_avg
    FROM
        weather_data
    WHERE
        YEAR(date) = <input_year> - 1
    GROUP BY
        MONTH(date)
    ) AS previous_year
JOIN
    (
    SELECT
        MONTH(date) AS month,
        AVG(temperature) AS current_year_avg
    FROM
        weather_data
     WHERE
        YEAR(date) = <input_year>
    GROUP BY
        MONTH(date)
    ) AS current_year ON previous_year.month = current_year.month;










## _Exercise 2 - Packaging_
This exercise contains a Dockerfile and other necessary files to create a Docker image that can run the previous exercises. The Docker image is based on Linux and includes Python 3.8+ with package management using requirements.txt file

## Code Breakdown

- First the base Docker image, which is the official Python 3.8+ runtime is created which provides necessary environment to run the above Python script.
- A working directory `/app` is created inside the container  where the application files will be copied and executed.
- `requirements.txt` file copies file from the local directory to the container's working directory which in our case is `/app`
- Now navigate through src/Dockerfile and build image which will download all the dependencies in the docker ([screenshot of the terminal tab when building an image](https://photos.app.goo.gl/Lh6P2L3snuTRMR4fA))
- Then open a docker and run the image built on it which gives the desired outputs 
- [Click here for the output of a docker file.](https://photos.app.goo.gl/mfrvEByw5y7sc3nq9)
 






##			Thank You :)

		
I am grateful for the chance to contribute to this Assignment and learned new things beside the skills that i possess.

##
