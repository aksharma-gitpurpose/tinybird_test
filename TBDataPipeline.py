import configparser, os, wget
import pandas as pd
import numpy as np
import logging

class Config:
    def __init__(self):        
        configObj = configparser.ConfigParser()
        filePath = os.path.join('config','config.ini')
        configObj.read(filePath)
        self.configObj = configObj        
        
   
class URLDataExtractor:
    #Responsible for Extracting data from a source and extract it to a destination file
    def __init__(self, config):
        self.sourceURL = config.configObj.get('ingestion', 'file_url')
        self.destFileName = config.configObj.get('ingestion','raw_datafile')
        self.logger = logging.getLogger('URLDataExtractor')        
        logging.basicConfig(level=config.configObj.get('logging','level'))

    def extractData(self):
        self.logger.info("Deleting file before downloading")
        try:
            os.remove(self.destFileName)
        except Exception as e:
            self.logger.info('No file exists to delete')

        self.logger.info(f"Extracting data from URL {self.sourceURL}")
        try:
            wget.download(self.sourceURL, self.destFileName)
        except Exception as e:
            self.logger.critical('Error when downloading file')
            raise e
        self.logger.info(f"Finished with downloading to {self.destFileName}")

        
class DataCleaner:

    def __init__(self, config):
        self.sourceFileName = config.configObj.get('ingestion', 'raw_datafile')
        self.stagingFileName = config.configObj.get('staging','staging_datafile')
        self.logger = logging.getLogger('DataCleaner')
        logging.basicConfig(level=config.configObj.get('logging','level'))

    def cleanData(self):
        self.logger.info('Cleaning the raw dataset')
        df = self.readData()

        df = self.filterData(df)

        self.writeData(df)        

        self.logger.info("Done writing transformed data to parquet file")

    def readData(self):        
        try: 
            return pd.read_parquet(self.sourceFileName)
        except Exception as e:
            self.logger.error('Error in reading parquet file')
            raise e

    def writeData(self, df):
        #Delete pre-existing staging file (if non empty director)
        self.logger.info('Deleting pre-existing staging datafile')
        try:
            os.remove(self.stagingFileName)
        except Exception as e: 
            self.logger.info('Not able to remove staging file. Probably file didnt exist')            

        try:
            df.to_parquet(self.stagingFileName)
        except Exception as e:
            self.logger.error('Not able to write staging file')
            raise e


    def filterData(self, df):
        #1. Filter out records with 0 trip distance.         
        self.logger.info(f"Filter out records where trip distance is less than or equals to 0")
        df = df[df['trip_distance'] > 0]
        self.logger.info(f"After filtering the number of rows left is {df['trip_distance'].count()}")

        #2. Filter out records with null trip distance.         
        self.logger.info("Filtering out records with null trip distance")
        df = df[df['trip_distance'].notnull()]
        self.logger.info(f"After filtering out number of rows left is {df['trip_distance'].count()}")

        #3. Filter out records with trip distance > 50 miles.         
        self.logger.info("Filtering out outliers i.e. Trips with distance > 50 miles")
        df = df[df['trip_distance'] <= 50]
        self.logger.info(f"After filtering out number of rows left is {df['trip_distance'].count()}")

        return df
        
    
class DataTransformer: 

    def __init__(self, config):
        self.config = config
        self.fileToRead = config.configObj.get('staging','staging_datafile')
        self.fileToWrite = config.configObj.get('transform','analysis_datafile')
        self.logger = logging.getLogger('DataTransformer')
        logging.basicConfig(level=self.config.configObj.get('logging','level'))
        

    #Method to transform
    def transform(self):

        self.logger.info("Reading parquet file for analysis")
        df = pd.read_parquet(self.fileToRead)

        #Calculate 90 percentile threshold 
        percentile_value = self.config.configObj.get('transform','percentile_value')
        try: 
            percent_val = float(percentile_value)
        except Exception as e: 
            self.logger.warn('Input percentil is not a float number. Resorting to default value of 90 percentile')
            percent_val = 90
        threshold_value = np.percentile(df['trip_distance'], percent_val)
        self.logger.info(f"90th Percentile of trip_distance is {threshold_value}")
        

        #Filter data with trip distance > threshold
        self.logger.info("Filtering out records that are less than 90th percentile")
        dfResult = df[df['trip_distance'] > threshold_value]
        self.logger.info(f"Number of trips with > 90th percentile trip distance is {dfResult['trip_distance'].count()}")

        #Remove the analysis file before writing 
        try:
            os.remove(self.fileToWrite)
        except Exception as e:
            self.logger.info("Not able to delete analysis file. File may not be existing")
        
        try:
            dfResult.to_parquet(self.fileToWrite)
        except Exception as e:
            self.logger.error('Not able to write transformed file')
            raise e
   
        self.logger.info('Done writing analysis file')  
        self.summaryCount = dfResult['trip_distance'].count()
        self.resultDataSetName = self.fileToWrite




class DataPipeline: 
    def __init__(self):
        self.config = Config()
        self.extractor = URLDataExtractor(self.config)
        self.cleaner = DataCleaner(self.config)
        self.transformer = DataTransformer(self.config)
        self.logger = logging.getLogger('DataPipeline')
        logging.basicConfig(level=self.config.configObj.get('logging','level'))


    #Orchestrates the various components together (Extractor, Cleaner and Transformer)
    def run(self):
        
        self.logger.info("Starting Extract of data")
        self.extractor.extractData()

        self.logger.info("Starting Cleaning of data")
        self.cleaner.cleanData()

        self.logger.info("Starting Transform of data")
        self.transformer.transform()        

        self.logger.info("Pipeline execution completed")


    def displaySummary(self):
        print(f'*******************************************************************************')
        print(f'**********************        SUMMARY           *******************************')
        print(f'*******************************************************************************')
        print()
        print(f'Data Set has been written to {self.transformer.fileToWrite}')
        print(f'Number of records in the result data set = {self.transformer.summaryCount}')
        print()
        print(f'*******************************************************************************')




if __name__ == '__main__':
    pipeline = DataPipeline()
    pipeline.run()
    pipeline.displaySummary()
