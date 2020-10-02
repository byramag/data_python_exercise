import json
import os

class DataTransformer:

    def __init__(self, spark, schema_path='resources/report_columns.json'):
        """ Initializes a DataTransformer object to handle any schema management of data """
        self.spark = spark
        self.schema_path = schema_path
        self.schema = self.load_output_schema(schema_path)

    def load_output_schema(self, schema_path):
        """ Loads a local file to read the correct output schema for this report """
        with open(self.schema_path, 'r') as schema_file:
            schema = json.load(schema_file)
        return schema

    def fit_output_schema(self, df):
        """ Given a Spark dataframe, reformat it to fit the given output schema """
        df = df.select(*self.schema.keys())
        for col in df.columns:
            df = df.withColumnRenamed(col, self.schema[col])
        return df