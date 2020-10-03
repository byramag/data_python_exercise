"""
Module test_io_handler.py: combines unit test cases for the IOHandler class functions
"""
import os

from ..io_handler import IOHandler


def test_init(spark):
    """ Tests IO handler init function with valid Spark session """
    io_handler = IOHandler(spark)
    assert io_handler

def test_spark_read_file_csv(spark):
    """ Tests spark_read_file with valid CSV input """
    path = os.getcwd() + '/data_processor/tests/test_data/test.csv'
    read_df = IOHandler(spark).spark_read_file(file_path=path)
    assert read_df.count() == 2

def test_spark_read_file_csv_delim(spark):
    """ Tests spark_read_file with valid CSV input including delimiter """
    path = os.getcwd() + '/data_processor/tests/test_data/test.csv'
    delim = ','
    read_df = IOHandler(spark).spark_read_file(path, delim)
    assert read_df.count() == 2

def test_spark_read_file_json(spark):
    """ Tests spark_read_file with valid JSON input """
    path = os.getcwd() + '/data_processor/tests/test_data/test.json'
    delim = ','
    read_df = IOHandler(spark).spark_read_file(path, delim)
    assert read_df.count() == 3

def test_spark_read_file_no_ext(spark):
    """ Tests spark_read_file with file missing extension """
    path = os.getcwd() + '/data_processor/tests/test_data/test'
    delim = ','
    read_df = IOHandler(spark).spark_read_file(path, delim)
    assert read_df is None

def test_write_report(spark):
    """ Tests write_report with valid output data """
    out_path = os.getcwd() + '/data_processor/tests/test_data/test_report.json'
    write_df = spark.createDataFrame(
        [(1, 2, 3), (4, 5, 6)],
        ['a', 'b', 'c']
    )
    wrote_file = IOHandler(spark).write_report(write_df, 'json', out_path=out_path)
    assert wrote_file

def test_write_report_unsupported(spark):
    """ Tests write_report with invalid file type """
    write_df = spark.createDataFrame(
        [(1, 2, 3), (4, 5, 6)],
        ['a', 'b', 'c']
    )
    wrote_file = IOHandler(spark).write_report(write_df, 'txt')
    assert not wrote_file
