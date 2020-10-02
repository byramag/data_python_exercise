import re

class IOHandler:
    def __init__(self, spark=None):
        """ Initializes IOHandler object to perform any needed file I/O functions  """
        if spark:
            self.spark = spark
    
    def set_spark(self, spark):
        """ Helper function to set the Spark session for this object """
        self.spark = spark

    def spark_read_file(self, file_path, delim='', header=True):
        """ Read a file into a Spark dataframe with optional 
        formatting based on file extension
        """
        # Extracting file extension from path
        ext_search = re.search(r'\.(\w+)$', file_path)
        extension = ext_search.group(1) if ext_search else ''

        if extension == 'csv' and delim:
            df = self.spark.read.format(extension).option("delimiter", delim).option('header', 'true').csv(file_path)
        elif extension in ['parquet', 'csv', 'json']:
            df = self.spark.read.format(extension).load(file_path)
        else: # Generic file load
            df = self.spark.read.load(file_path)
            
        return df
    
    def write_report(self, df, out_format, out_path='report.json'):
        """ Write a Spark dataframe to a file if given a valid format """
        if out_format in ['json', 'csv', 'parquet']:
            df.write.format(out_format).mode('overwrite').save(out_path)
            return True
        return False