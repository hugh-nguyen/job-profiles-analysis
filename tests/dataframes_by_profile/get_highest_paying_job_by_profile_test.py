import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, sum
from modules.common import get_flattened_job_profile_data
from modules.dataframes_by_profile import get_highest_paying_job_by_profile


@pytest.fixture
def spark():
    spark = SparkSession.builder.appName('test').getOrCreate()
    yield spark
    spark.stop()


def test_get_highest_paying_job_by_profile(spark):
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
                        'salary': 104000,
                        'fromDate': '2018-08-08',
                    },
                    {
                        'title': 'dentist',
                        'location': 'Perth',
                        'salary': 44000,
                        'fromDate': '2017-08-08',
                    },
                ],
            },
        }
    ]

    sc = spark.sparkContext
    df = spark.read.option('inferSchema', 'true').json(sc.parallelize(data))
    df = get_flattened_job_profile_data(df)

    result = get_highest_paying_job_by_profile(df)

    expected_data = [
        {
            'id': 'da313',
            'firstName': 'Daniel',
            'lastName': 'Doe',
            'highestPayingJobTitle': 'dentist',
            'highestPayingJobSalary': 104000,
            'highestPayingJobYear': 2018,
        }
    ]

    expected = spark.createDataFrame(expected_data, result.schema)

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0


def test_get_highest_paying_job_by_profile_multiple_highest_salaries(spark):
    data = [
        {
            'id': 'da313',
            'profile': {
                'firstName': 'Daniel',
                'lastName': 'Doe',
                'jobHistory': [
                    {
                        'title': 'senior dentist',
                        'location': 'Perth',
                        'salary': 104000,
                        'fromDate': '2019-08-08',
                    },
                    {
                        'title': 'dentist',
                        'location': 'Perth',
                        'salary': 104000,
                        'fromDate': '2018-08-08',
                    },
                    {
                        'title': 'dentist',
                        'location': 'Perth',
                        'salary': 44000,
                        'fromDate': '2017-08-08',
                    },
                ],
            },
        }
    ]

    sc = spark.sparkContext
    df = spark.read.option('inferSchema', 'true').json(sc.parallelize(data))
    df = get_flattened_job_profile_data(df)

    result = get_highest_paying_job_by_profile(df)
    result.show()

    expected_data = [
        {
            'id': 'da313',
            'firstName': 'Daniel',
            'lastName': 'Doe',
            'highestPayingJobTitle': 'senior dentist',
            'highestPayingJobSalary': 104000,
            'highestPayingJobYear': 2019,
        },
        {
            'id': 'da313',
            'firstName': 'Daniel',
            'lastName': 'Doe',
            'highestPayingJobTitle': 'dentist',
            'highestPayingJobSalary': 104000,
            'highestPayingJobYear': 2018,
        },
    ]

    expected = spark.createDataFrame(expected_data, result.schema)

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0
