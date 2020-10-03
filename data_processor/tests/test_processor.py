"""
Module test_processor.py
"""
import os
import pytest
from .. import processor

test_dir = os.getcwd() + '/data_processor/tests/test_data/'

def test_join_dfs(spark):
    """ Test join_dfs function with valid parameters """
    df1 = spark.createDataFrame(
        [(1, 2, 3), (4, 5, 6)],
        ['col1', 'col2', 'col3']
    )
    df2 = spark.createDataFrame(
        [(1, 2, 3), (4, 5, 6)],
        ['col1', 'col4', 'col5']
    )
    joined_df = processor.join_dfs(df1, df2, 'col1')
    assert set(joined_df.columns) == {'s_col1', 't_col1', 's_col2', 's_col3', 't_col4', 't_col5'}

def test_run(spark):
    """ Test run function with valid parameters """
    student_path = test_dir + 'test.csv'
    teacher_path = test_dir + 'test.json'
    out_path = test_dir + 'test_out.json'
    processor.run(spark, student_path, teacher_path, out_path)
    assert 'test_out.json' in os.listdir(test_dir)

def test_run_no_file(spark):
    """ Test run function with invalid file path parameters """
    student_path = 'not_exists.json'
    teacher_path = 'not_exists.json'
    with pytest.raises(Exception):
        processor.run(spark, student_path, teacher_path)
