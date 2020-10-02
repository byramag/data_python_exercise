import json
import os

class DataTransformer:

    def __init__(self, spark, schema_path='resources/report_columns.json'):
        self.spark = spark
        self.schema_path = schema_path
        self.schema = self.load_output_schema(schema_path)

    def load_output_schema(self, schema_path):
        with open(self.schema_path, 'r') as schema_file:
            schema = json.load(schema_file)
        return schema

    def fit_output_schema(self, df):
        df = df.select(*self.schema.keys())
        for col in df.columns:
            df = df.withColumnRenamed(col, self.schema[col])
        return df