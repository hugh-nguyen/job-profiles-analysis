import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, sum
from modules.common import get_flattened_job_profile_data
from modules.dataframes_by_profile import get_average_salary_for_all_profiles


@pytest.fixture
def spark():
    spark = SparkSession.builder.appName('test').getOrCreate()
    yield spark
    spark.stop()


def test_get_average_salary_for_all_profiles(spark):
    data = [
        {
            'id': 'da313',
            'profile': {
                'firstName': 'Jane',
                'lastName': 'Dee',
                'jobHistory': [
                    {
                        'title': 'dentist',
                        'location': 'Perth',
                        'salary': 20000,
                        'fromDate': '2019-08-08',
                    },
                    {
                        'title': 'dentist',
                        'location': 'Perth',
                        'salary': 30000,
                        'fromDate': '2016-02-08',
                        'toDate': '2019-08-08',
                    },
                    {
                        'title': 'dentist',
                        'location': 'Perth',
                        'salary': 50000,
                        'fromDate': '2016-02-08',
                        'toDate': '2019-08-08',
                    },
                ],
            },
        },
        {
            'id': 'da314',
            'profile': {
                'firstName': 'John',
                'lastName': 'Doe',
                'jobHistory': [
                    {
                        'title': 'dentist',
                        'location': 'Perth',
                        'salary': 10000,
                        'fromDate': '2019-08-08',
                    },
                    {
                        'title': 'dentist',
                        'location': 'Perth',
                        'salary': 20000,
                        'fromDate': '2019-08-08',
                    },
                    {
                        'title': 'dentist',
                        'location': 'Perth',
                        'salary': 20010,
                        'fromDate': '2016-02-08',
                        'toDate': '2019-08-08',
                    },
                ],
            },
        },
    ]

    sc = spark.sparkContext
    df = spark.read.option('inferSchema', 'true').json(sc.parallelize(data))
    df = get_flattened_job_profile_data(df)

    result = get_average_salary_for_all_profiles(df)

    expected_data = [{'avgSalary': 25001.67}]

    expected = spark.createDataFrame(expected_data, result.schema)

    assert result.subtract(expected).count() == 0
    assert expected.subtract(result).count() == 0
