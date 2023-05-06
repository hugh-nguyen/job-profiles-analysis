from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    asc,
    avg,
    col,
    count as get_count,
    desc,
    explode,
    isnull,
    lower,
    min,
    max,
    round,
    sum,
    year as get_year
)
from pyspark.sql.window import Window


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


def get_average_salaries_by_profile(df: DataFrame) -> DataFrame:
    """
    Returns a DataFrame with average salaries grouped by profile.
    Args:
        df (DataFrame): Input DataFrame.
    Returns:
        DataFrame: DataFrame with average salaries grouped by profile.
    """
    result = df.groupBy('id', 'firstName', 'lastName') \
        .agg(avg('jobDetail.salary').alias('avgSalary')) \
        .orderBy('avgSalary')

    result = result.withColumn('avgSalary', round(result['avgSalary'], 2))

    return result


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


def get_average_salaries_by_job_title(df: DataFrame) -> DataFrame:
    """
    Returns a DataFrame with average salaries grouped by job title.
    Args:
        df (DataFrame): Input DataFrame.
    Returns:
        DataFrame: DataFrame with average salaries grouped by job title.
    """
    result = df.groupBy(lower('jobDetail.title').alias('jobTitle')) \
        .agg(avg('jobDetail.salary').alias('avgSalary'))

    result = result.withColumn('avgSalary', round(result['avgSalary'], 2))

    return result


def get_current_salaries_by_profile(df: DataFrame) -> DataFrame:
    """
    Returns a DataFrame with current salaries grouped by profile.
    Args:
        df (DataFrame): Input DataFrame.
        column_name (str): Name of the column to find the maximum value.
    Returns:
        DataFrame: DataFrame with rows containing the maximum value for the given column.
    """
    result = df.where(isnull(col('jobDetail.toDate'))) \
        .groupBy('id', 'firstName', 'lastName') \
        .agg(sum('jobDetail.salary').alias('currentSalary'))

    result = result.withColumn(
        'currentSalary',
        round(result['currentSalary'], 2)
    )
    
    return result


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


def get_first_seen_dates_by_title(df: DataFrame) -> DataFrame:
    """
    Returns a DataFrame with the first seen dates for each job title.
    Args:
        df (DataFrame): Input DataFrame.
    Returns:
        DataFrame: DataFrame with the first seen dates for each job title.
    """
    result = df.groupBy('jobDetail.title') \
        .agg(min(col('jobDetail.fromDate')).alias('firstSeenDate'))

    return result


def get_most_popular_job_titles(df: DataFrame, year: int = None) -> DataFrame:
    """
    Returns a DataFrame with the most popular job titles, optionally filtered by year.
    Args:
        df (DataFrame): Input DataFrame.
        year (int, optional): Year to filter the results. Defaults to None.
    Returns:
        DataFrame: DataFrame with the most popular job titles.
    """
    title_first_seen_dates = get_first_seen_dates_by_title(df)
    if year:
        title_first_seen_dates = title_first_seen_dates.where(
            get_year(col('firstSeenDate')) == year
        )

    title_counts = df.groupBy('jobDetail.title') \
        .agg(get_count('jobDetail.title').alias('occurrence'))

    result = title_first_seen_dates.join(title_counts, 'title', 'inner')

    return result.orderBy(desc('occurrence'))


def get_all_current_jobs(df: DataFrame) -> DataFrame:
    """
    Returns a DataFrame with all current jobs (where 'toDate' is null).
    Args:
        df (DataFrame): Input DataFrame.
    Returns:
        DataFrame: DataFrame with all current jobs.
    """
    return df.where(isnull(col('jobDetail.toDate')))


def get_most_recent_jobs_by_profile(df: DataFrame) -> DataFrame:
    """
    Returns a DataFrame with the most recent jobs for each profile.
    Args:
        df (DataFrame): Input DataFrame.
    Returns:
        DataFrame: DataFrame with the most recent jobs for each profile.
    """
    df_max_dates = df.groupBy('id') \
        .agg(max('jobDetail.fromDate').alias('maxFromDate'))

    result = df.join(df_max_dates, on=['id']) \
        .where(col('jobDetail.fromDate') == col('maxFromDate')) \
        .select('id', 'firstName', 'lastName', 'jobDetail')

    return result


def get_highest_paying_job_by_profile(df: DataFrame) -> DataFrame:
    """
    Returns a DataFrame with the highest paying job for each profile.
    Args:
        df (DataFrame): Input DataFrame.
    Returns:
        DataFrame: DataFrame with the highest paying job for each profile.
    """
    result = df.withColumn(
        'highestSalary',
        max(col('jobDetail.salary')).over(Window.partitionBy("id"))
    )
    result = result.where(col('jobDetail.salary') == col('highestSalary'))

    return result.selectExpr(
        'id',
        'firstName',
        'lastName',
        'jobDetail.title AS highestPayingJobTitle',
        'jobDetail.salary AS highestPayingJobSalary',
        'YEAR(jobDetail.fromDate) AS highestPayingJobYear'
    )
