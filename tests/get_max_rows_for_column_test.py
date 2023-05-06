from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pytest

from util import get_max_rows_for_column

@pytest.fixture
def spark():
    spark = SparkSession.builder.appName('test').getOrCreate()
    yield spark
    spark.stop()

def test_get_max_rows_for_age_column_simple(spark):

    data = [
        {
            'Name': 'John',
            'Age': 30
        },
        {
            'Name': 'Alice',
            'Age': 35
        },
        {
            'Name': 'Emma',
            'Age': 28
        },
    ]
    sc = spark.sparkContext
    df = spark.read.option('inferSchema', 'true').json(sc.parallelize(data))

    result = get_max_rows_for_column(df, 'Age')
    result.show()

    expected_data = [
        {
            'Name': 'Alice',
            'Age': 35
        },
    ]
    expected = spark.createDataFrame(expected_data, result.schema)

    assert result.collect() == expected.collect()


def test_get_max_rows_for_age_column_multiple_max_vals(spark):

    data = [
        {
            'Name': 'John',
            'Age': 30
        },
        {
            'Name': 'Alice',
            'Age': 35
        },
        {
            'Name': 'Cassie',
            'Age': 35
        },
        {
            'Name': 'Bob',
            'Age': 35
        },
        {
            'Name': 'Emma',
            'Age': 28
        },
    ]
    sc = spark.sparkContext
    df = spark.read.option('inferSchema', 'true').json(sc.parallelize(data))

    result = get_max_rows_for_column(df, 'Age')
    result.show()

    expected_data = [
        {
            'Name': 'Alice',
            'Age': 35
        },
        {
            'Name': 'Cassie',
            'Age': 35
        },
        {
            'Name': 'Bob',
            'Age': 35
        },
    ]
    expected = spark.createDataFrame(expected_data, result.schema)

    assert result.collect() == expected.collect()
