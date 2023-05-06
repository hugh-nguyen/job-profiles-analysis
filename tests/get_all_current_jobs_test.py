import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, sum
from util import (
    get_flattened_job_profile_data,
    get_all_current_jobs
)


@pytest.fixture
def spark():
    spark = SparkSession.builder.appName('test').getOrCreate()
    yield spark
    spark.stop()

base_data = [
    {
        'id': 'da312',
        'profile': {
            'firstName': 'Moon',
            'lastName': 'Yong',
            'jobHistory': [
                {
                    'title': 'dentist',
                    'location': 'Perth',
                    'salary': 104000,
                    'fromDate': '2012-08-08',
                    'toDate': '2019-01-01'
                }
            ]
        }
    }
]

def test_get_all_current_jobs_one_current_job(spark):

    data = base_data + [
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
                    }
                ]
            }
        }
    ]

    sc = spark.sparkContext
    df = spark.read.option('inferSchema', 'true').json(sc.parallelize(data))
    df = get_flattened_job_profile_data(df)

    result = get_all_current_jobs(df)

    expected_data = [
        {
            'id': 'da313',
            'firstName': 'Daniel',
            'lastName': 'Doe',
            'jobDetail': {
                'title': 'dentist',
                'location': 'Perth',
                'salary': 104000,
                'fromDate': '2019-08-08',
            }
        }
    ]

    expected = spark.createDataFrame(expected_data, result.schema)

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0


def test_get_all_current_jobs_no_current_jobs(spark):

    data = base_data + [
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
                        'toDate': '2019-09-08'
                    }
                ]
            }
        }
    ]

    sc = spark.sparkContext
    df = spark.read.option('inferSchema', 'true').json(sc.parallelize(data))
    df = get_flattened_job_profile_data(df)

    result = get_all_current_jobs(df)
    expected = spark.createDataFrame([], result.schema)

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0


def test_get_all_current_jobs_one_current_and_one_previous_job(spark):

    data = base_data + [
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
                        'fromDate': '2018-08-08',
                        'toDate': '2019-08-08',
                    }
                ]
            }
        }
    ]

    sc = spark.sparkContext
    df = spark.read.option('inferSchema', 'true').json(sc.parallelize(data))
    df = get_flattened_job_profile_data(df)

    result = get_all_current_jobs(df)

    expected_data = [
        {
            'id': 'da313',
            'firstName': 'Daniel',
            'lastName': 'Doe',
            'jobDetail': {
                'title': 'dentist',
                'location': 'Perth',
                'salary': 104000,
                'fromDate': '2019-08-08',
            }
        }
    ]

    expected = spark.createDataFrame(expected_data, result.schema)

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0
