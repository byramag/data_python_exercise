"""
Module conftest.py: helper for pytest execution
"""
import pytest

import findspark
findspark.init()

from pyspark.sql.session import SparkSession # Must be after findspark.init()

@pytest.fixture(scope="session")
def spark():
    """ Returns spark session for shared test use """
    return SparkSession.builder.getOrCreate()
