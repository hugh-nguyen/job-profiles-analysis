from modules.decorators import log
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    explode,
    isnull,
    max,
    year as get_year
)


@log
def get_all_current_jobs(df: DataFrame) -> DataFrame:
    """
    Returns a DataFrame with all current jobs (where 'toDate' is null).
    Args:
        df (DataFrame): Input DataFrame.
    Returns:
        DataFrame: DataFrame with all current jobs.
    """
    return df.where(isnull(col('jobDetail.toDate')))


@log
def get_flattened_job_profile_data(df: DataFrame) -> DataFrame:
    """
    Flatten job profile data for further processing.
    Args:
        df (DataFrame): Input DataFrame.
    Returns:
        DataFrame: lattened DataFrame with job details exploded.
    """
    result = df.select(
        'id',
        col('profile.firstName').alias('firstName'),
        col('profile.lastName').alias('lastName'),
        explode('profile.jobHistory').alias('jobDetail')
    )

    return result


@log
def get_max_rows_for_column(df, column_name):
    """
    Returns a DataFrame with the max row for a column, if there are ties
        then return multiple rows.
    Args:
        df (DataFrame): Input DataFrame.
    Returns:
        DataFrame: DataFrame with the max row(s) for a column
    """
    max_value = df.agg(max(column_name)).collect()[0][0]
    max_rows = df.filter(col(column_name) == max_value)

    return max_rows

