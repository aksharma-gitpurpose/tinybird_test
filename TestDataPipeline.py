import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

import unittest
from unittest.mock import patch, MagicMock
from TBDataPipeline import URLDataExtractor, DataCleaner, DataTransformer, DataPipeline, Config
import logging
import pandas as pd
import numpy as np

class TestDataPipeline(unittest.TestCase):

    @patch('TBDataPipeline.URLDataExtractor')  # Mocking the URLDataExtractor
    @patch('TBDataPipeline.DataCleaner')  # Mocking the DataCleaner
    @patch('TBDataPipeline.DataTransformer')  # Mocking the DataTransformer
    @patch('TBDataPipeline.Config')  # Mocking the Config
    @patch('logging.getLogger', return_value=logging.getLogger('DataPipeline'))  # Mocking logging
    def test_pipeline_run(self, mock_logger, mock_config, mock_data_transformer, mock_data_cleaner, mock_url_data_extractor):
        # Arrange
        mock_config.return_value = MagicMock()  # Create a mock config object
        mock_config.return_value.configObj.get.return_value = "INFO"  # Return INFO for log level

        # Create mock instances for each component
        mock_extractor_instance = mock_url_data_extractor.return_value
        mock_cleaner_instance = mock_data_cleaner.return_value
        mock_transformer_instance = mock_data_transformer.return_value

        # Create the pipeline instance
        pipeline = DataPipeline()

        # Act
        pipeline.run()

        # Assert
        # Check if extractData was called
        mock_extractor_instance.extractData.assert_called_once()

        # Check if cleanData was called
        mock_cleaner_instance.cleanData.assert_called_once()

        # Check if transform was called
        mock_transformer_instance.transform.assert_called_once()        