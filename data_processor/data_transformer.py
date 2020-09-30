import json

class DataTransformer:

    def __init__(self, spark, schema_path='resources/report_schema.json'):
        self.spark = spark
        self.schema_path = schema_path
        self.schema = self.load_output_schema(schema_path)

    def load_output_schema(self, schema_path):
        schema = json.load(self.schema_path)
        return schema

    def fit_output_schema(self, df):
        df = df.select(self.schema.keys())
        for col in df.cols:
            df = df.withColumnRenamed(self.schema[col])
        return df