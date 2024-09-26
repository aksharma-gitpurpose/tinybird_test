import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

import unittest
from unittest.mock import patch, MagicMock
from TBDataPipeline import URLDataExtractor, Config
import logging

class TestURLDataExtractor(unittest.TestCase):
    
    @patch('wget.download')  # Mock the download function
    @patch('configparser.ConfigParser')  # Mock the configparser to simulate config settings
    def test_extractData(self, mock_config_parser, mock_download):
        # Set up the mock for ConfigParser
        mock_config = MagicMock()
        mock_config.get.side_effect = lambda section, key: {
            ('ingestion', 'file_url'): 'http://example.com/data.csv',
            ('ingestion', 'raw_datafile'): 'raw_data.csv',
            ('logging', 'level'): 'INFO'
        }[(section, key)]
        
        # Mock configparser instance to return our mock config object
        mock_config_parser.return_value = mock_config
        
        # Create a Config object (which uses the mocked configparser)
        config = Config()
        
        # Initialize URLDataExtractor with the mocked Config
        extractor = URLDataExtractor(config)
        
        # Call the method we want to test
        extractor.extractData()

        # Assertions
        # Assert that wget.download was called with the correct URL and destination file
        mock_download.assert_called_once_with('http://example.com/data.csv', 'raw_data.csv')
        

if __name__ == '__main__':
    unittest.main()