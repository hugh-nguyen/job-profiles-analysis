from modules.decorators import log
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    isnull,
    max,
    round,
    sum,
)
from pyspark.sql.window import Window


@log
def get_average_salaries_by_profile(df: DataFrame) -> DataFrame:
    """
    Returns a DataFrame with average salaries grouped by profile.
    Args:
        df (DataFrame): Input DataFrame.
    Returns:
        DataFrame: DataFrame with average salaries grouped by profile.
    """
    result = (
        df.groupBy('id', 'firstName', 'lastName')
        .agg(avg('jobDetail.salary').alias('avgSalary'))
        .orderBy('avgSalary')
    )

    result = result.withColumn('avgSalary', round(result['avgSalary'], 2))

    return result


@log
def get_average_salary_for_all_profiles(df: DataFrame) -> DataFrame:
    """
    Returns a DataFrame with the average salary for all profiles.
    Args:
        df (DataFrame): Input DataFrame.
    Returns:
        DataFrame: DataFrame with the average salary for all profiles.
    """
    result = df.agg(avg('jobDetail.salary').alias('avgSalary'))

    result = result.withColumn('avgSalary', round(result['avgSalary'], 2))

    return result


@log
def get_current_salaries_by_profile(df: DataFrame) -> DataFrame:
    """
    Returns a DataFrame with current salaries grouped by profile.
    Args:
        df (DataFrame): Input DataFrame.
        column_name (str): Name of the column to find the maximum value.
    Returns:
        DataFrame: DataFrame with rows containing the maximum value for the given column.
    """
    result = (
        df.where(isnull(col('jobDetail.toDate')))
        .groupBy('id', 'firstName', 'lastName')
        .agg(sum('jobDetail.salary').alias('currentSalary'))
    )

    result = result.withColumn('currentSalary', round(result['currentSalary'], 2))

    return result


@log
def get_highest_paying_job_by_profile(df: DataFrame) -> DataFrame:
    """
    Returns a DataFrame with the highest paying job for each profile.
    Args:
        df (DataFrame): Input DataFrame.
    Returns:
        DataFrame: DataFrame with the highest paying job for each profile.
    """
    result = df.withColumn(
        'highestSalary', max(col('jobDetail.salary')).over(Window.partitionBy("id"))
    )
    result = result.where(col('jobDetail.salary') == col('highestSalary'))

    return result.selectExpr(
        'id',
        'firstName',
        'lastName',
        'jobDetail.title AS highestPayingJobTitle',
        'jobDetail.salary AS highestPayingJobSalary',
        'YEAR(jobDetail.fromDate) AS highestPayingJobYear',
    )


@log
def get_most_recent_jobs_by_profile(df: DataFrame) -> DataFrame:
    """
    Returns a DataFrame with the most recent jobs for each profile.
    Args:
        df (DataFrame): Input DataFrame.
    Returns:
        DataFrame: DataFrame with the most recent jobs for each profile.
    """
    df_max_dates = df.groupBy('id').agg(max('jobDetail.fromDate').alias('maxFromDate'))

    result = (
        df.join(df_max_dates, on=['id'])
        .where(col('jobDetail.fromDate') == col('maxFromDate'))
        .select('id', 'firstName', 'lastName', 'jobDetail')
    )

    return result
