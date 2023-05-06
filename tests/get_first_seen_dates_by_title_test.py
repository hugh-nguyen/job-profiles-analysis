import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, sum
from util import (
    get_flattened_job_profile_data,
    get_first_seen_dates_by_title
)


@pytest.fixture
def spark():
    spark = SparkSession.builder.appName('test').getOrCreate()
    yield spark
    spark.stop()


def test_get_first_seen_dates_by_title(spark):

    data = [
        {
            'id': 'da313',
            'profile': {
                'firstName': 'Daniel',
                'lastName': 'Doe',
                'jobHistory': [
                    {
                        'title': 'dentist',
                        'location': 'Perth',
                        'salary': 104000,
                        'fromDate': '2019-08-08',
                    },
                    {
                        'title': 'dentist',
                        'location': 'Perth',
                        'salary': 104000,
                        'fromDate': '2016-08-08',
                    }
                ]
            }
        },
        {
            'id': 'da314',
            'profile': {
                'firstName': 'Don',
                'lastName': 'Legend',
                'jobHistory': [
                    {
                        'title': 'dentist',
                        'location': 'Perth',
                        'salary': 104000,
                        'fromDate': '2016-01-08',
                    }
                ]
            }
        },
        {
            'id': 'da315',
            'profile': {
                'firstName': 'Don',
                'lastName': 'Legend',
                'jobHistory': [
                    {
                        'title': 'dentist',
                        'location': 'Perth',
                        'salary': 104000,
                        'fromDate': '2017-01-08',
                    },
                    {
                        'title': 'handy man',
                        'location': 'Perth',
                        'salary': 104000,
                        'fromDate': '2014-01-08',
                    }
                ]
            }
        }
    ]

    sc = spark.sparkContext
    df = spark.read.option('inferSchema', 'true').json(sc.parallelize(data))
    df = get_flattened_job_profile_data(df)

    result = get_first_seen_dates_by_title(df)

    expected_data = [
        {
            'title': 'dentist',
            'firstSeenDate': '2016-01-08'
        },
        {
            'title': 'handy man',
            'firstSeenDate': '2014-01-08'
        },
    ]

    expected = spark.createDataFrame(expected_data, result.schema)

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0