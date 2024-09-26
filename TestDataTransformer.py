import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

import unittest
from unittest.mock import patch, MagicMock
from TBDataPipeline import DataTransformer, Config
import logging
import pandas as pd
import numpy as np

class TestDataTransformer(unittest.TestCase):

    def getMockConfig(self):

        mock_config = MagicMock()
        mock_config.configObj.get.side_effect = lambda section, key: {
            ('staging', 'staging_datafile'): 'staging_data.parquet',
            ('transform', 'analysis_datafile'): 'analysis_data.parquet',
            ('transform', 'percentile_value'): '90',
            ('logging', 'level'): 'INFO'
        }[(section, key)]        

        return mock_config

    @patch('pandas.read_parquet', autospec=True)  # Mocking pandas.read_parquet
    @patch('pandas.DataFrame.to_parquet', autospec=True)  # Mocking DataFrame's to_parquet method
    @patch('os.remove', autospec=True)  # Mocking os.remove
    def test_transform(self, mock_remove, mock_to_parquet, mock_read_parquet):
        # Sample DataFrame with trip distances
        sample_data = {
            'trip_distance': [5, 15, 25, 35, 45, 55, 65, 75, 85, 95]
        }
        sample_df = pd.DataFrame(sample_data)

        # Mocking read_parquet to return the sample DataFrame
        mock_read_parquet.return_value = sample_df

        # Mocking configuration object
        mock_config = self.getMockConfig()        

        # Initialize DataTransformer object
        transformer = DataTransformer(mock_config)

        # Call transform method
        transformer.transform()

        # Assert that read_parquet was called with the correct filename
        mock_read_parquet.assert_called_once_with('staging_data.parquet')

        # Calculate expected threshold value
        expected_threshold = np.percentile(sample_df['trip_distance'], 90)
        
        # Check if filtering is done correctly
        expected_filtered_df = sample_df[sample_df['trip_distance'] > expected_threshold]        

        # Ensure os.remove was called with the correct filename
        mock_remove.assert_called_once_with('analysis_data.parquet')

            