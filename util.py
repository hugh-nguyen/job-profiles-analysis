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


def get_flattened_job_profile_data(df):

    result = df.select(
        'id',
        col('profile.firstName').alias('firstName'),
        col('profile.lastName').alias('lastName'),
        explode('profile.jobHistory').alias('jobDetail')
    )

    return result


def get_average_salaries_by_profile(df):

    result = df.groupBy('id', 'firstName', 'lastName') \
        .agg(avg('jobDetail.salary').alias('avgSalary')) \
        .orderBy('avgSalary')

    result = result.withColumn('avgSalary', round(result['avgSalary'], 2))

    return result


def get_average_salary_for_all_profiles(df):

    result = df.agg(avg('jobDetail.salary').alias('avgSalary'))

    result = result.withColumn('avgSalary', round(result['avgSalary'], 2))

    return result


def get_average_salaries_by_job_title(df):

    result = df.groupBy(lower('jobDetail.title').alias('jobTitle')) \
        .agg(avg('jobDetail.salary').alias('avgSalary'))
    
    result = result.withColumn('avgSalary', round(result['avgSalary'], 2))
    
    return result


def get_current_salaries_by_profile(df):

    result = df.where(isnull(col('jobDetail.toDate'))) \
        .groupBy('id', 'firstName', 'lastName') \
        .agg(sum('jobDetail.salary').alias('currentSalary'))

    result = result.withColumn(
        'currentSalary',
        round(result['currentSalary'], 2)
    )
    
    return result


def get_max_rows_for_column(df, column_name):

    max_value = df.agg(max(column_name)).collect()[0][0]
    max_rows = df.filter(col(column_name) == max_value)

    return max_rows


def get_first_seen_dates_by_title(df):

    result = df.groupBy('jobDetail.title') \
        .agg(min(col('jobDetail.fromDate')).alias('firstSeenDate'))
    
    return result


def get_most_popular_job_titles(df, year=None):

    title_first_seen_dates = get_first_seen_dates_by_title(df)
    if year:
        title_first_seen_dates = title_first_seen_dates.where(
            get_year(col('firstSeenDate')) == year
        )
    
    title_counts = df.groupBy('jobDetail.title') \
        .agg(get_count('jobDetail.title').alias('occurrence'))
    
    result = title_first_seen_dates.join(title_counts, 'title', 'inner')
    
    return result.orderBy(desc('occurrence'))