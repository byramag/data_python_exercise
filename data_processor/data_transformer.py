"""
Module data_transformer.py
"""
import json

class DataTransformer:
    """ Class DataTransformer:
    manages any transformations needed on the data
    """

    def __init__(self, spark, schema_path='resources/report_columns.json'):
        """ Initializes a DataTransformer object to handle any schema management of data """
        self.spark = spark
        self.schema_path = schema_path
        self.schema = self.load_output_schema()

    def load_output_schema(self, schema_path=''):
        """ Loads a local file to read the correct output schema for this report """
        if not schema_path:
            schema_path = self.schema_path

        with open(schema_path, 'r') as schema_file:
            schema = json.load(schema_file)
        return schema

    def fit_output_schema(self, out_df):
        """ Given a Spark dataframe, reformat it to fit the given output schema """
        out_df = out_df.select(*self.schema.keys())
        for col in out_df.columns:
            out_df = out_df.withColumnRenamed(col, self.schema[col])
        return out_df
