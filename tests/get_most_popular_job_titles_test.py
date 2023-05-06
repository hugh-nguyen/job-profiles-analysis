import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, sum
from util import (
    get_flattened_job_profile_data,
    get_most_popular_job_titles
)


@pytest.fixture
def spark():
    spark = SparkSession.builder.appName('test').getOrCreate()
    yield spark
    spark.stop()


shared_data = [
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
                    'fromDate': '2015-08-08',
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
                    'title': 'doctor',
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
                    'title': 'engineer',
                    'location': 'Perth',
                    'salary': 104000,
                    'fromDate': '2016-07-08',
                },
                {
                    'title': 'doctor',
                    'location': 'Perth',
                    'salary': 104000,
                    'fromDate': '2015-07-08',
                },
                {
                    'title': 'handy man',
                    'location': 'Perth',
                    'salary': 104000,
                    'fromDate': '2015-01-08',
                },
            ]
        }
    }
]


def test_get_most_popular_job_titles_overall(spark):

    data = shared_data
    sc = spark.sparkContext
    df = spark.read.option('inferSchema', 'true').json(sc.parallelize(data))
    df = get_flattened_job_profile_data(df)

    result = get_most_popular_job_titles(df)
    
    expected_data = [
        {
            'title': 'dentist',
            'firstSeenDate': '2015-08-08',
            'occurrence': 3
        },
        {
            'title': 'doctor',
            'firstSeenDate': '2015-07-08',
            'occurrence': 2
        },
        {
            'title': 'handy man',
            'firstSeenDate': '2015-01-08',
            'occurrence': 1
        },
        {
            'title': 'engineer',
            'firstSeenDate': '2016-07-08',
            'occurrence': 1
        },
    ]

    expected = spark.createDataFrame(expected_data, result.schema)

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0


def test_get_most_popular_job_titles_empty_years(spark):

    data = shared_data
    sc = spark.sparkContext
    df = spark.read.option('inferSchema', 'true').json(sc.parallelize(data))
    df = get_flattened_job_profile_data(df)

    result = get_most_popular_job_titles(df, 2014)
    expected = spark.createDataFrame([], result.schema)

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0

    result = get_most_popular_job_titles(df, 2017)
    expected = spark.createDataFrame([], result.schema)

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0


def test_get_most_popular_job_titles_specific_year(spark):

    data = shared_data
    sc = spark.sparkContext
    df = spark.read.option('inferSchema', 'true').json(sc.parallelize(data))
    df = get_flattened_job_profile_data(df)

    result = get_most_popular_job_titles(df, 2015)

    expected_data = [
        {
            'title': 'dentist',
            'firstSeenDate': '2015-08-08',
            'occurrence': 3
        },
        {
            'title': 'doctor',
            'firstSeenDate': '2015-07-08',
            'occurrence': 2
        },
        {
            'title': 'handy man',
            'firstSeenDate': '2015-01-08',
            'occurrence': 1
        },
    ]

    expected = spark.createDataFrame(expected_data, result.schema)

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0


def test_get_most_popular_job_titles_engineer_overrides_dentist_due_to_first_seen(spark):

    data = shared_data
    sc = spark.sparkContext
    df = spark.read.option('inferSchema', 'true').json(sc.parallelize(data))
    df = get_flattened_job_profile_data(df)

    result = get_most_popular_job_titles(df, 2016)

    expected_data = [
        {
            'title': 'engineer',
            'firstSeenDate': '2016-07-08',
            'occurrence': 1
        },
    ]

    expected = spark.createDataFrame(expected_data, result.schema)

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0