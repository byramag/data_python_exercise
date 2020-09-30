import re

class IOHandler:
    def __init__(self, spark):
        self.spark = spark

    def spark_read_file(self, file_path, delim=''):

        # Extracting file extension from path
        extension = re.search(r'\.(\w+)$', file_path).group(1)

        if extension == 'csv' and delim:
            df = self.spark.read.option("delimiter", delim).csv(file_path)
        elif extension in ['parquet', 'csv', 'json']:
            df = self.spark.read.format(extension).load(file_path)
        else: # Generic file load
            df = self.spark.read.load(file_path)
            
        return df
    
    def write_report(self, df, out_format, out_path='report.json'):
        if out_format in ['json', 'csv', 'parquet']:
            df.write.format(out_format).save(out_path)
            return True
        return False