import logging
from pyspark.sql import functions as F

from io_handler import IOHandler
from data_transformer import DataTransformer

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

def join_dfs(student_df, teacher_df, join_key):
    student_df = student_df.select([F.col(c).alias('s_' + c) for c in student_df.columns])
    teacher_df = teacher_df.select([F.col(c).alias('t_' + c) for c in teacher_df.columns])

    joined_df = student_df.join(teacher_df, on=student_df["s_" + join_key] == teacher_df["t_" + join_key], how='left')
    return joined_df

def run(spark, student_file, teacher_file, out_path='report.json'):
    """ Main driver function of data processor application """

    io = IOHandler(spark)
    try:
        student_df = io.spark_read_file(student_file, delim='_')
        logger.info("Successfully loaded student file from " + student_file)
        teacher_df = io.spark_read_file(teacher_file)
        logger.info("Successfully loaded teacher file from " + teacher_file)
    except FileNotFoundError as e:
        logger.error(e)
        return

    transformer = DataTransformer(spark)
    joined_df = join_dfs(student_df, teacher_df, 'cid')
    logger.info("Finished joining dataframes")
    output_df = transformer.fit_output_schema(joined_df)
    logger.info("Fit data to output schema:")
    output_df.show()

    io.write_report(output_df, 'json', out_path)
    
