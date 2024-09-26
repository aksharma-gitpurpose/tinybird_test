import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import pandas as pd
import numpy as np
import logging
from TBDataPipeline import DataCleaner, Config


class TestDataCleaner(unittest.TestCase):

    def getMockConfig(self):

        mock_config = MagicMock()
        mock_config.configObj.get.side_effect = lambda section, key: {
            ('ingestion', 'raw_datafile'): 'raw_data.parquet',
            ('staging', 'staging_datafile'): 'staging_data.parquet',
            ('logging', 'level'): 'INFO'
        }[(section, key)]
        mock_config.configObj.level.return_value = logging.INFO

        return mock_config


    @patch('pandas.read_parquet', autospec=True)  # Mocking pandas.read_parquet    
    def test_readData(self, mock_read_parquet):
        # Mocked return value of read_parquet
        sample_df = pd.DataFrame({'trip_distance': [10, 20, 30]})
        mock_read_parquet.return_value = sample_df

        # Mocking configuration object
        mock_config = self.getMockConfig()

        # Initialize DataCleaner object
        cleaner = DataCleaner(mock_config)

        # Call readData and check the return value
        result_df = cleaner.readData()

        # Assert that read_parquet was called with the correct filename
        mock_read_parquet.assert_called_once_with('raw_data.parquet')

        # Assert that the returned DataFrame matches the sample_df
        pd.testing.assert_frame_equal(result_df, sample_df)

    @patch('os.remove', autospec=True)  # Mocking os.remove
    @patch('pandas.DataFrame.to_parquet', autospec=True)  # Mocking DataFrame's to_parquet method
    def test_writeData(self, mock_to_parquet, mock_remove):
        # Sample DataFrame
        sample_df = pd.DataFrame({'trip_distance': [10, 20, 30]})

        # Mocking configuration object
        mock_config = self.getMockConfig()

        # Initialize DataCleaner object
        cleaner = DataCleaner(mock_config)

        # Call writeData
        cleaner.writeData(sample_df)

        # Check that os.remove was called with the correct staging filename
        mock_remove.assert_called_once_with('staging_data.parquet')

        # Ensure the DataFrame was written to parquet
        mock_to_parquet.assert_called_once_with(sample_df, 'staging_data.parquet')

    def test_filterData(self):
        # Sample DataFrame with edge cases for filtering
        sample_df = pd.DataFrame({
            'trip_distance': [0, 10, np.nan, 51, 25, 30]
        })

        # Mocking configuration object
        mock_config = self.getMockConfig()
        
        # Initialize DataCleaner object
        cleaner = DataCleaner(mock_config)

        # Call filterData
        filtered_df = cleaner.filterData(sample_df)

        # Expected DataFrame after filtering
        expected_filtered_df = pd.DataFrame({'trip_distance': [10.0, 25, 30]})

        # Assert that the filtered DataFrame matches the expected output        
        pd.testing.assert_frame_equal(filtered_df.reset_index(drop=True), expected_filtered_df.reset_index(drop=True))

    @patch('pandas.read_parquet', autospec=True)  # Mocking pandas.read_parquet
    @patch('os.remove', autospec=True)  # Mocking os.remove
    @patch('pandas.DataFrame.to_parquet', autospec=True)  # Mocking DataFrame's to_parquet method
    def test_cleanData(self, mock_to_parquet, mock_remove, mock_read_parquet):
        # Sample DataFrame before cleaning
        sample_df = pd.DataFrame({
            'trip_distance': [0, 10, np.nan, 51, 25, 30]
        })

        # Mocking read_parquet to return our sample DataFrame
        mock_read_parquet.return_value = sample_df

        # Mocking configuration object
        mock_config = self.getMockConfig()        

        # Initialize DataCleaner object
        cleaner = DataCleaner(mock_config)

        # Call cleanData
        cleaner.cleanData()

        # Ensure read_parquet was called correctly
        mock_read_parquet.assert_called_once_with('raw_data.parquet')

        # Ensure os.remove was called with the correct staging filename
        mock_remove.assert_called_once_with('staging_data.parquet')         
    
if __name__ == '__main__':
    unittest.main()
