# New York Yellow Taxi Trip Analysis

The repo contains files which when run will perform some Data Engineering Tasks and some basic analysis on trips data from [NYC Yellow Taxi.](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) 
The basic Data Engineering Tasks include: 
- Extraction of parquet files from a URL ([NYC Yello Taxi Trips Site](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)) 
- Some basic cleaning of data. 

The analysis tasks include 
- Filtering out records that have trip_distance greater than .9 percentile.

The output of the program will be a parquet data set that contains trip details where trip_distance > 0.9 percentile. 

## Table of Contents
- [Assumptions made](#assumptions-made)
- [Flow of the program](#flow-of-the-program)
- [Code Descriptions](#code-descriptions)
    - [config](#configconfigini)
    - [dependencies](#requirementstxt)
    - [Dockerfile](#dockerfile)
    - [TBDataPipeline](#tbdatapipelinepy)
    - [TestFiles](#testpy)
- [How to run the code](#how-to-run-the-code)
    - [Running Locally](#running-the-program-locally)
    - [Run using docker](#running-the-program-by-building-a-docker-image)
- [How to test the code](#how-to-test-the-code)
- [Production Usage](#production-usage)
- [Troubleshooting](#troubleshooting)


## Assumptions made

Two Major Assumption: 
- The assement says to find trips that are > 0.9 percentile. I am assuming that it means 90 percentile. 
- I have used Python 3 to write the code. I am assuming that python 3 is available with the team that executes this code.

There are some assumptions made when analysing the data and they include: 

- All records where trip_distance <= 0 are excluded from analysis. 
- All records where trip_distance > 50 miles are excluded from analysis.(This is because NYC is 35 miles wide at the widest part and Yellow Taxis operation only within NYC)

There are other inconsistencies in the dataset but we will consider them as wrong data entry by taxi driver and will include them in the calculation as it is. More clarity can be obtained after much discussion on whether to eliminate these records or include them or fix them. For the purpose of this exercise, I am including them in the dataset for analysis

- All records with negative fare is included. 
- All records where passenger count is 0 or > 5 are also included. 

From Data Engineering point of view, my thoughts while preparing this dataset is: 

- Raw data is downloaded from source and is not in a useable state. 
- We clean this data and create a useable dataset that can be used by multiple analyst. This dataset is created in a staging area. 
- This particular use case of finding trips > .90 percentile are specific to a anlysis team and is created as a different dataset. It is created as a data set and not returned to dispaly or HTML page because the results are huge and might not be feasible to be analysed manually. 


## Flow of the program 

The python program TBDataPipeline.py starts by getting some configurations values. These values include: 
- The source file to download 
- The raw data set name to be created 
- The staging / cleaned dataset name to be created. 
- The final analysis dataset to be created. 
- The logging level to be used. 

The program then does the following: 
- Downloads the parquet file from URL and saves it as the raw dataset. 
- Reads the raw data set and cleans it by removing records with 0 or negative trip_distance and also those where trip_distance > 50. 
- It also removes those records where trip_distance is null
- It then calculates the 90th percentile value for trip_distance column 
- It then filters in records that are > 90th percentile of trip_distance. 
- Finally it prints a summary that includes the name of the dataset created and also the count of records in that dataset. 

## Code Descriptions 

### config/config.ini 
This is a configuration file and includes config values for:
```
[ingestion]
file_url=URL of the parquet file on webserver
raw_datafile=Dataset name for the downloaded parquet file

[staging]
staging_datafile=Parquet File Name for Cleaned data set

[transform]
analysis_datafile=Parquet File Name for Result Data Set
percentile_value=Percentile threshold value for trip_distance

[logging]
level=Log Level to be used ( DEBUG, INFO, WARN ...)
``` 

### requirements.txt 
This is python requirements file and includes the dependencies required to run this program. 

### Dockerfile 
This is the docker file used to build docker image of this program

### TBDataPipeline.py 
This is the main program that performes DataExtraction/Cleaning and Filtering to to get the Result Data Set. 


### Test*.py 
There are 4 Test python files that tests the various classes in TBDataPipeline. All of them follow the pattern Test*.py

## How to run the code 

### Running the program locally 
Prerequisite: 
- Need to have python3 installed

Steps: 
1. Create a python virtual environment and activate it
``` 
python -m venv tbenv

On Windows Command Prompt (execute): > tbenv\Scripts\activate 


On Mac: $source ./tbenv/Scripts/activate
```

2. Navigate to the root folder that contains the program TBDataPipeline.py

3. Install the dependencies 

```
pip install --no-cache-dir -r requirements.txt
```

4. Run the program 

```
python TBDataPipeline.py
```

5. On Successful completion you may see the following output (last few lines): 

```
INFO:DataPipeline:Pipeline execution completed
*******************************************************************************
**********************        SUMMARY           *******************************
*******************************************************************************

Data Set has been written to tmp_90percentile_trip_distance.parquet
Number of records in the result data set = 290139

*******************************************************************************
```

### Running the program by building a docker image

Prerequisite 
- Have docker desktop installed 

Steps: 
1. Navigate to the root folder that contains Docker file. 
2. Build docker image

```
docker build -t tinybird_pipeline --no-cache . 
```

3. Run docker image 

```
docker run -it tinybird_pipeline /bin/bash
``` 

4. Inside the container execute the following 

```
python TBDataPipeline.py 
``` 

5. On Successful completion you may see the following output (last few lines): 

```
INFO:DataPipeline:Pipeline execution completed
*******************************************************************************
**********************        SUMMARY           *******************************
*******************************************************************************
Data Set has been written to tmp_90percentile_trip_distance.parquet
Number of records in the result data set = 290139

*******************************************************************************
```

## How to test the code 

The program may be tested locally or on docker by executing: 

```
python -m unittest TestURLExtractor
python -m unittest TestDataCleaner
python -m unittest TestDataTransformer
python -m unittest TestDataPipeline
``` 

## Production Usage
The program in this repo is just a POC and is certainly not suitable for production use case. Here are some pointers on changes to be done for such an application for its usage in Production. 

### Data Extraction
1. There are various different types of data sources, therefore the Data Extraction module needs to be able to handle variety of data sources. 
2. State Management module needs to be added to Data Extractors for sources that support Change Data Capture. 
3. Authentication module needs to be added for sources that requires Authenticated requests. 
4. Pagination Support for sources that return only a subset of data at at ime. 
5. Rate Limit Handling for sources that prevent too frequent requests. 
6. Capturing stats and Monitoring of pipelines also needs to be added. 

### Data Processing 
1. Large datasets cannot be processed using Pandas, we will need to switch to Spark Cluster and use PySpark or Scala to do the data transformation
2. If data is loaded to a warehouse we could also use DBT / SQLMesh for data transformation. 

### Testing 
1. More tests cases needs to be covered. 
2. End 2 End tests cases also needs to be added. 



## Troubleshooting 
1. Extraction doesn't work because of network issues. 
It may be possible that due to corporate network restrictions the extraction process fails. 
If this happens then do the following: 
- Manually download the parquet file and save it as raw_taxi_trip_data.parquet in the root folder. 
- Comment the line no 162 in TBDataPipeline.py

```
#self.extractor.extractData()
```








