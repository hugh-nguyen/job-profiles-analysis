import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, sum
from util import (
    get_flattened_job_profile_data,
    get_average_salaries_by_job_title
)


@pytest.fixture
def spark():
    spark = SparkSession.builder.appName('test').getOrCreate()
    yield spark
    spark.stop()


def test_get_average_salaries_by_job_title_simple(spark):

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
                        'salary': 98000,
                        'fromDate': '2016-02-08',
                        'toDate': '2019-08-08'
                    }
                ]
            }
        },
        {
            'id': 'da314',
            'profile': {
                'firstName': 'John',
                'lastName': 'Moe',
                'jobHistory': [
                    {
                        'title': 'dentist',
                        'location': 'Perth',
                        'salary': 78000,
                        'fromDate': '2019-08-08',
                    },
                    {
                        'title': 'dentist',
                        'location': 'Perth',
                        'salary': 65000,
                        'fromDate': '2016-02-08',
                        'toDate': '2019-08-08'
                    }
                ]
            }
        }
    ]

    df = spark.read.option('inferSchema', 'true').json(spark.sparkContext.parallelize(data))
    df = get_flattened_job_profile_data(df)

    result = get_average_salaries_by_job_title(df)

    expected_data = [
        {
            'jobTitle': 'dentist',
            'avgSalary': 86250.0
        }
    ]

    expected = spark.createDataFrame(expected_data, result.schema)
    result.show()
    expected.show()

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0


def test_get_average_salaries_by_job_title_with_decimal_place_check(spark):

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
                        'salary': 50000,
                        'fromDate': '2019-08-08',
                    },
                    {
                        'title': 'dentist',
                        'location': 'Perth',
                        'salary': 20000,
                        'fromDate': '2016-02-08',
                        'toDate': '2019-08-08'
                    }
                ]
            }
        },
        {
            'id': 'da314',
            'profile': {
                'firstName': 'John',
                'lastName': 'Moe',
                'jobHistory': [
                    {
                        'title': 'dentist',
                        'location': 'Perth',
                        'salary': 30000,
                        'fromDate': '2019-08-08',
                    },
                    {
                        'title': 'doctor',
                        'location': 'Perth',
                        'salary': 30000,
                        'fromDate': '2019-08-08',
                    },
                ]
            }
        },
        {
            'id': 'da315',
            'profile': {
                'firstName': 'John',
                'lastName': 'Gan',
                'jobHistory': [
                    {
                        'title': 'doctor',
                        'location': 'Perth',
                        'salary': 10000,
                        'fromDate': '2019-08-08',
                    },
                    {
                        'title': 'doctor',
                        'location': 'Perth',
                        'salary': 10000,
                        'fromDate': '2019-08-08',
                    },
                ]
            }
        }
    ]

    df = spark.read.option('inferSchema', 'true').json(spark.sparkContext.parallelize(data))
    df = get_flattened_job_profile_data(df)

    result = get_average_salaries_by_job_title(df)
    result.show()

    expected_data = [
        {
            'jobTitle': 'dentist',
            'avgSalary': 33333.33
        },
        {
            'jobTitle': 'doctor',
            'avgSalary': 16666.67
        }
    ]

    expected = spark.createDataFrame(expected_data, result.schema)

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0