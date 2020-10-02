from pyspark.sql import SparkSession
import pytest
import os

from ..data_transformer import DataTransformer

class TestDataTransformer:
    spark = SparkSession.builder.getOrCreate()
    transformer = DataTransformer(self.spark, os.getcwd() + '/data_processor/tests/test_data/test_schema.json')

    def test_load_output_schema(self):
        path = os.getcwd() + '/data_processor/tests/test_data/test_schema.json'
        schema = self.transformer.load_output_schema(schema_path=path)
        assert schema.keys() == ['col1', 'col2']
    
    def test_load_output_schema_no_file(self):
        path = 'doesnt_exist.json'
        with pytest.raises(FileNotFoundError):
            schema = self.transformer.load_output_schema(schema_path=path)

    def test_fit_output_schema(self):
        df = self.spark.createDataFrame(
            [(1,2), (4,5)],
            ['col1','col2']
        )
        df = self.transformer.fit_output_schema(df)
        assert df.columns == ['column1', 'column2']

    def test_fit_output_schema_missing_cols(self):
        df = self.spark.createDataFrame(
            [(1,2), (4,5)],
            ['col1','col3']
        )
        # with pytest.raises():
        df = self.transformer.fit_output_schema(df)
        