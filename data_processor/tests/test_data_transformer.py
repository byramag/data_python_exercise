"""
Module test_data_transformer: combines unit test cases for the DataTransformer class functions
"""
import os
import pytest

from ..data_transformer import DataTransformer

schema_path = os.getcwd() + '/data_processor/tests/test_data/test_schema.json'

def test_init(spark):
    """ Tests IO handler init function with valid Spark session """
    transformer = DataTransformer(spark, schema_path)
    assert transformer

def test_load_output_schema(spark):
    """ Tests the load_output_schema function with valid path """
    schema = DataTransformer(spark, schema_path)\
        .load_output_schema(schema_path=schema_path)
    assert set(schema.keys()) == {'col1', 'col2'}

def test_load_output_schema_no_file(spark):
    """ Tests the load_output_schema function with non existent path """
    path = 'doesnt_exist.json'
    with pytest.raises(FileNotFoundError):
        DataTransformer(spark).load_output_schema(schema_path=path)

def test_fit_output_schema(spark):
    """ Tests the fit_output_schema function with valid input dataframe """
    in_df = spark.createDataFrame(
        [(1, 2), (4, 5)],
        ['col1', 'col2']
    )
    out_df = DataTransformer(spark, schema_path).fit_output_schema(in_df)
    assert set(out_df.columns) == {'column1', 'column2'}

def test_fit_schema_lack_cols(spark):
    """ Tests the fit_output_schema function with invalid input dataframe
    (missing required columns in schema)
    """
    in_df = spark.createDataFrame(
        [(1, 2), (4, 5)],
        ['col1', 'col3']
    )
    with pytest.raises(Exception):
        DataTransformer(spark, schema_path).fit_output_schema(in_df)
