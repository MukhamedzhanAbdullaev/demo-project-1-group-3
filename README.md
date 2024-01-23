# Air Quality - ETL Project
Created by group 3:

Alissa Cheng

Alan Segal

Mukhamedzhan Abdullaev

## Project Overviev
### Context
This project connects to an API that allows the extraction of Population and air quality measurements of a specific city across the globe.
### Goal 
- It aims to create business solutions as they relate to population, population density, and air quality in cities selected from a CSV file.
- This data pipeline would be valuable to businesses that prioritize respiratory healthcare, air filtering technology, or real estate brokers who target healthy living
## Overview of the code
### Assets air_quality.py
- extract() function
  - Perform extraction using an API which has live data on each city
- extract_cities_data()
  - Perform extraction using a CSV file to get data on most populated cities in the world
- transform() function
  - City names set to lowercase for merging
  - Merging csv to air quality data frame
  - Select columns to use
  - Create rank column where higher rank = better air quality
  - Create population density column: population / km2
  - Drop area column
  - Rename columns to meaningful labels
  - Create categories based on air quality score
- load() function
  - Load dataframe to either a database
### Pipelines air_quality.py
- The entire process of ETL can be executed with a single call of this file
## Resources
### Data source:
- Air Quality API Extraction: Live Dataset https://api.waqi.info/feed
- CSV file of cities: Static Dataset city_data.csv
### Enviroment:
- Python 3.9
### Dependencies:
- Import dependencies using file requirements.txt 
### Software:
- PostgreSQL and PgAdmin
- Docker
- AWS
