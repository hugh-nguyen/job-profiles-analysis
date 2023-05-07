import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, sum
from modules.common import get_flattened_job_profile_data


@pytest.fixture
def spark():
    spark = SparkSession.builder.appName('test').getOrCreate()
    yield spark
    spark.stop()


def test_flattened_job_profile_data_simple(spark):
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
                        'salary': 103000,
                        'fromDate': '2016-02-08',
                        'toDate': '2019-08-08',
                    }
                ],
            },
        }
    ]

    sc = spark.sparkContext
    df = spark.read.option('inferSchema', 'true').json(sc.parallelize(data))

    result = get_flattened_job_profile_data(df)

    expected_data = [
        {
            'id': 'da313',
            'firstName': 'Daniel',
            'lastName': 'Doe',
            'jobDetail': {
                'title': 'dentist',
                'location': 'Perth',
                'salary': 103000,
                'fromDate': '2016-02-08',
                'toDate': '2019-08-08',
            },
        }
    ]

    expected = spark.createDataFrame(expected_data, result.schema)

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0


def test_flattened_job_profile_data_more_profiles(spark):
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
                        'salary': 103000,
                        'fromDate': '2016-02-08',
                        'toDate': '2019-08-08',
                    },
                    {
                        'title': 'dentist',
                        'location': 'Melbourne',
                        'salary': 113000,
                        'fromDate': '2014-02-08',
                        'toDate': '2016-01-08',
                    },
                ],
            },
        }
    ]

    sc = spark.sparkContext
    df = spark.read.option('inferSchema', 'true').json(sc.parallelize(data))

    result = get_flattened_job_profile_data(df)

    expected_data = [
        {
            'id': 'da313',
            'firstName': 'Daniel',
            'lastName': 'Doe',
            'jobDetail': {
                'title': 'dentist',
                'location': 'Perth',
                'salary': 103000,
                'fromDate': '2016-02-08',
                'toDate': '2019-08-08',
            },
        },
        {
            'id': 'da313',
            'firstName': 'Daniel',
            'lastName': 'Doe',
            'jobDetail': {
                'title': 'dentist',
                'location': 'Melbourne',
                'salary': 113000,
                'fromDate': '2014-02-08',
                'toDate': '2016-01-08',
            },
        },
    ]

    expected = spark.createDataFrame(expected_data, result.schema)

    result.show()
    expected.show()

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0


def test_flattened_job_profile_data_more_job_history(spark):
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
                        'salary': 103000,
                        'fromDate': '2016-02-08',
                        'toDate': '2019-08-08',
                    },
                    {
                        'title': 'dentist',
                        'location': 'Melbourne',
                        'salary': 113000,
                        'fromDate': '2014-02-08',
                        'toDate': '2016-01-08',
                    },
                ],
            },
        },
        {
            'id': 'da314',
            'profile': {
                'firstName': 'Jone',
                'lastName': 'Woe',
                'jobHistory': [
                    {
                        'title': 'doctor',
                        'location': 'Perth',
                        'salary': 111000,
                        'fromDate': '2016-02-08',
                        'toDate': '2019-08-08',
                    }
                ],
            },
        },
        {
            'id': 'da315',
            'profile': {
                'firstName': 'Jane',
                'lastName': 'Goe',
                'jobHistory': [
                    {
                        'title': 'anchor',
                        'location': 'Perth',
                        'salary': 111000,
                        'fromDate': '2016-02-08',
                        'toDate': '2019-08-08',
                    },
                    {
                        'title': 'engineer',
                        'location': 'Perth',
                        'salary': 1121000,
                        'fromDate': '2016-02-08',
                        'toDate': '2019-08-08',
                    },
                    {
                        'title': 'anchor',
                        'location': 'Perth',
                        'salary': 115000,
                        'fromDate': '2016-02-08',
                        'toDate': '2019-08-08',
                    },
                ],
            },
        },
    ]

    sc = spark.sparkContext
    df = spark.read.option('inferSchema', 'true').json(sc.parallelize(data))

    result = get_flattened_job_profile_data(df)

    expected_data = [
        {
            'id': 'da313',
            'firstName': 'Daniel',
            'lastName': 'Doe',
            'jobDetail': {
                'title': 'dentist',
                'location': 'Perth',
                'salary': 103000,
                'fromDate': '2016-02-08',
                'toDate': '2019-08-08',
            },
        },
        {
            'id': 'da313',
            'firstName': 'Daniel',
            'lastName': 'Doe',
            'jobDetail': {
                'title': 'dentist',
                'location': 'Melbourne',
                'salary': 113000,
                'fromDate': '2014-02-08',
                'toDate': '2016-01-08',
            },
        },
        {
            'id': 'da314',
            'firstName': 'Jone',
            'lastName': 'Woe',
            'jobDetail': {
                'title': 'doctor',
                'location': 'Perth',
                'salary': 111000,
                'fromDate': '2016-02-08',
                'toDate': '2019-08-08',
            },
        },
        {
            'id': 'da315',
            'firstName': 'Jane',
            'lastName': 'Goe',
            'jobDetail': {
                'title': 'anchor',
                'location': 'Perth',
                'salary': 111000,
                'fromDate': '2016-02-08',
                'toDate': '2019-08-08',
            },
        },
        {
            'id': 'da315',
            'firstName': 'Jane',
            'lastName': 'Goe',
            'jobDetail': {
                'title': 'engineer',
                'location': 'Perth',
                'salary': 1121000,
                'fromDate': '2016-02-08',
                'toDate': '2019-08-08',
            },
        },
        {
            'id': 'da315',
            'firstName': 'Jane',
            'lastName': 'Goe',
            'jobDetail': {
                'title': 'anchor',
                'location': 'Perth',
                'salary': 115000,
                'fromDate': '2016-02-08',
                'toDate': '2019-08-08',
            },
        },
    ]

    expected = spark.createDataFrame(expected_data, result.schema)

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0
