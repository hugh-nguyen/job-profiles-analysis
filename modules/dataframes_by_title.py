from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count as get_count,
    desc,
    lower,
    min,
    round,
    year as get_year
)


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