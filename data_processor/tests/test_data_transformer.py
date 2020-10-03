"""
Module test_data_transformer
"""
import os
import pytest

from ..data_transformer import DataTransformer

class TestDataTransformer:
    """ Class TestDataTransformer:
    combines unit test cases for the DataTransformer class functions
    """

    transformer = None

    def test_init(self, spark):
        """ Tests IO handler init function with valid Spark session """
        self.transformer = DataTransformer(
            spark,
            os.getcwd() + '/data_processor/tests/test_data/test_schema.json'
        )

    def test_load_output_schema(self):
        """ Tests the load_output_schema function with valid path """
        path = os.getcwd() + '/data_processor/tests/test_data/test_schema.json'
        schema = self.transformer.load_output_schema(schema_path=path)
        assert schema.keys() == ['col1', 'col2']

    def test_load_output_schema_no_file(self):
        """ Tests the load_output_schema function with non existent path """
        path = 'doesnt_exist.json'
        with pytest.raises(FileNotFoundError):
            self.transformer.load_output_schema(schema_path=path)

    def test_fit_output_schema(self, spark):
        """ Tests the fit_output_schema function with valid input dataframe """
        in_df = spark.createDataFrame(
            [(1, 2), (4, 5)],
            ['col1', 'col2']
        )
        out_df = self.transformer.fit_output_schema(in_df)
        assert out_df.columns == ['column1', 'column2']

    def test_fit_schema_lack_cols(self, spark):
        """ Tests the fit_output_schema function with invalid input dataframe
        (missing required columns in schema)
        """
        in_df = spark.createDataFrame(
            [(1, 2), (4, 5)],
            ['col1', 'col3']
        )
        with pytest.raises(Exception):
            self.transformer.fit_output_schema(in_df)
        