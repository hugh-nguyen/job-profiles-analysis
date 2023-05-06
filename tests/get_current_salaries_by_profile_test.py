import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, sum
from util import (
    get_flattened_job_profile_data,
    get_current_salaries_by_profile
)


@pytest.fixture
def spark():
    spark = SparkSession.builder.appName('test').getOrCreate()
    yield spark
    spark.stop()


def test_get_current_salaries_by_profile_simple(spark):

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
                        'salary': 10000,
                        'fromDate': '2016-02-08',
                        'toDate': '2019-08-08'
                    }
                ]
            }
        }
    ]

    df = spark.read.option('inferSchema', 'true').json(spark.sparkContext.parallelize(data))
    df = get_flattened_job_profile_data(df)
    
    result = get_current_salaries_by_profile(df)

    expected_data = [
        {
            'id': 'da313',
            'firstName': 'Daniel',
            'lastName': 'Doe',
            'currentSalary': 104000
        }
    ]

    expected = spark.createDataFrame(expected_data, result.schema)

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0


def test_get_current_salaries_by_profile_where_profile_has_no_current_job(spark):

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
                        'fromDate': '2019-80-08',
                    },
                    {
                        'title': 'dentist',
                        'location': 'Perth',
                        'salary': 10000,
                        'fromDate': '2016-02-08',
                        'toDate': '2019-08-08'
                    }
                ]
            }
        },
        {
            'id': 'da314',
            'profile': {
                'firstName': 'Joe',
                'lastName': 'Chain',
                'jobHistory': [
                    {
                        'title': 'clerk',
                        'location': 'Perth',
                        'salary': 101200,
                        'fromDate': '2019-08-08',
                        'toDate': '2019-08-08'
                    },
                    {
                        'title': 'store clerk',
                        'location': 'Perth',
                        'salary': 104000,
                        'fromDate': '2016-02-08',
                        'toDate': '2019-08-08'
                    }
                ]
            }
        }
    ]

    sc = spark.sparkContext
    df = spark.read.option('inferSchema', 'true').json(sc.parallelize(data))
    df = get_flattened_job_profile_data(df)
    
    result = get_current_salaries_by_profile(df)

    expected_data = [
        {
            'id': 'da313',
            'firstName': 'Daniel',
            'lastName': 'Doe',
            'currentSalary': 104000
        }
    ]

    expected = spark.createDataFrame(expected_data, result.schema)

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0


def test_get_current_salaries_by_profile_where_profile_has_multiple_current_jobs(spark):

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
                        'fromDate': '2019-80-08',
                    },
                    {
                        'title': 'dentist',
                        'location': 'Perth',
                        'salary': 10000,
                        'fromDate': '2016-02-08',
                        'toDate': '2019-08-08'
                    }
                ]
            }
        },
        {
            'id': 'da314',
            'profile': {
                'firstName': 'Joe',
                'lastName': 'Chain',
                'jobHistory': [
                    {
                        'title': 'clerk',
                        'location': 'Perth',
                        'salary': 101200,
                        'fromDate': '2019-08-08',
                    },
                    {
                        'title': 'store clerk',
                        'location': 'Perth',
                        'salary': 104000,
                        'fromDate': '2016-02-08',
                    }
                ]
            }
        }
    ]

    sc = spark.sparkContext
    df = spark.read.option('inferSchema', 'true').json(sc.parallelize(data))
    df = get_flattened_job_profile_data(df)
    
    result = get_current_salaries_by_profile(df)

    expected_data = [
        {
            'id': 'da313',
            'firstName': 'Daniel',
            'lastName': 'Doe',
            'currentSalary': 104000
        },
        {
            'id': 'da314',
            'firstName': 'Joe',
            'lastName': 'Chain',
            'currentSalary': 205200
        }
    ]

    expected = spark.createDataFrame(expected_data, result.schema)

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0