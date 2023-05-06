import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, sum
from modules.common import get_flattened_job_profile_data
from modules.dataframes_by_profile import get_most_recent_jobs_by_profile

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


def test_get_most_recent_jobs_by_profile_with_one_current_job(spark):

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

    result = get_most_recent_jobs_by_profile(df)

    expected_data = [
        {
            'id': 'da312',
            'firstName': 'Moon',
            'lastName': 'Yong',
            'jobDetail': {
                'title': 'dentist',
                'location': 'Perth',
                'salary': 104000,
                'fromDate': '2012-08-08',
                'toDate': '2019-01-01'
            }
        },
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


def test_get_most_recent_jobs_by_profile_with_multiple_current_jobs(spark):

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
                        'fromDate': '2019-07-08',
                    }
                ]
            }
        }
    ]

    sc = spark.sparkContext
    df = spark.read.option('inferSchema', 'true').json(sc.parallelize(data))
    df = get_flattened_job_profile_data(df)

    result = get_most_recent_jobs_by_profile(df)

    expected_data = [
        {
            'id': 'da312',
            'firstName': 'Moon',
            'lastName': 'Yong',
            'jobDetail': {
                'title': 'dentist',
                'location': 'Perth',
                'salary': 104000,
                'fromDate': '2012-08-08',
                'toDate': '2019-01-01'
            }
        },
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


def test_get_most_recent_jobs_by_profile_with_multiple_current_jobs_single_finsished_job(spark):

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
                        'fromDate': '2019-09-08',
                        'toDate': '2019-09-08',
                    },
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
                        'fromDate': '2019-07-08',
                    }
                ]
            }
        }
    ]

    sc = spark.sparkContext
    df = spark.read.option('inferSchema', 'true').json(sc.parallelize(data))
    df = get_flattened_job_profile_data(df)

    result = get_most_recent_jobs_by_profile(df)

    expected_data = [
        {
            'id': 'da312',
            'firstName': 'Moon',
            'lastName': 'Yong',
            'jobDetail': {
                'title': 'dentist',
                'location': 'Perth',
                'salary': 104000,
                'fromDate': '2012-08-08',
                'toDate': '2019-01-01'
            }
        },
        {
            'id': 'da313',
            'firstName': 'Daniel',
            'lastName': 'Doe',
            'jobDetail': {
                'title': 'dentist',
                'location': 'Perth',
                'salary': 104000,
                'fromDate': '2019-09-08',
                'toDate': '2019-09-08',
            }
        }
    ]

    expected = spark.createDataFrame(expected_data, result.schema)

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0
