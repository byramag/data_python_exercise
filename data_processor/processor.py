import logging
from pyspark.sql import SparkSession

from .io_handler import IOHandler
from .data_transformer import DataTransformer

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()

def join_dfs(df1, df2, join_key_1, join_key_2):
    joined_df = df1.join(df2, on=df1[join_key_1] == df2[join_key_2], how='left')
    return joined_df

def run(student_file, teacher_file, out_path='report.json'):
    """ Main driver function of data processor application """

    io = IOHandler(spark)
    try:
        student_df = io.spark_read_file(student_file)
        logger.info("Successfully loaded student file from " + student_file)
        teacher_df = io.spark_read_file(teacher_file)
        logger.info("Successfully loaded teacher file from " + teacher_file)
    except FileNotFoundError as e:
        logger.error(e)
        return

    transformer = DataTransformer(spark)
    joined_df = join_dfs(student_df, teacher_df, 'id', 'student_id')
    output_df = transformer.fit_output_schema(joined_df)

    io.write_report(output_df, 'json')
    
