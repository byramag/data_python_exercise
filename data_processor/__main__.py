"""
Module __main__.py
"""
import argparse
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import processor

def spark_setup():
    """ Sets configuration and creates Spark session """
    conf = SparkConf()
    spark_context = SparkContext().getOrCreate(conf)
    spark_session = SparkSession(spark_context)
    spark_session.sparkContext.setLogLevel('WARN')
    return spark_session

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Input and output file names')
    parser.add_argument('student_file', help='the path to the file of student information')
    parser.add_argument('teacher_file', help='the path to the file of teacher information')
    parser.add_argument(
        '--out',
        dest='output_file',
        help='the desired output path for the json report'
    )
    input_args = parser.parse_args()

    print(input_args)

    spark = spark_setup()

    if input_args.output_file:
        processor.run(
            spark,
            input_args.student_file,
            input_args.teacher_file,
            input_args.output_file
        )
    else:
        processor.run(
            spark,
            input_args.student_file,
            input_args.teacher_file
        )
