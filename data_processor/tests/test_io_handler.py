from pyspark.sql import SparkSession

from ..io_handler import IOHandler

class TestIOHandler:
    spark = SparkSession.builder.getOrCreate()
    io = IOHandler(self.spark)

    def test_spark_read_file_csv(self):
        path = os.getcwd() + '/data_processor/tests/test_data/test.csv'
        df = self.io.spark_read_file(file_path=path)
        assert df.count == 2

    def test_spark_read_file_csv_delim(self):
        path = os.getcwd() + '/data_processor/tests/test_data/test.csv'
        delim = ','
        df = self.io.spark_read_file(path, delim)
        assert df.count == 2

    def test_spark_read_file_no_ext(self):
        path = os.getcwd() + '/data_processor/tests/test_data/test'
        delim = ','
        df = self.io.spark_read_file(path, delim)
        assert df.count == 3

    def test_write_report(self):
        out_path = os.getcwd() + '/data_processor/tests/test_data/test_report.json'
        df = self.spark.createDataFrame(
            [(1,2,3), (4,5,6)],
            ['a','b','c']
        )
        wrote_file = self.io.write_report(df, 'json', out_path=out_path)
        assert wrote_file

    def test_write_report_unsupported(self):
        df = self.spark.createDataFrame(
            [(1,2,3), (4,5,6)],
            ['a','b','c']
        )
        wrote_file = self.io.write_report(df, 'txt')
        assert not wrote_file