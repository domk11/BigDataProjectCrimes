from datetime import datetime
from random import randint

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DateType


def get_year(date):
    """Return the year.

    :param date: date in format yyyy-mm-dd
    :return: year
    :rtype: int
    """
    return int(str(date)[:4])


def get_month(date):
    """Return the month.

    :param date: date in format yyyy-mm-dd
    :return: month
    :rtype: int
    """
    return int(str(date).split('-')[1])


def parse_locus(locus_str):
    return 'OUTSIDE' if locus_str.upper() != 'INSIDE' else 'INSIDE'


def parse_daynight(time):
    """Parse time in day-night.

    :param time: time in format hh:mm:ss
    :return: 0 if day (6 am - 6 pm), 1 if night (7 pm - 5 am)
    :rtype: int
    """
    try:
        hour = int(time[:2])
        return 0 if (hour > 5) & (hour < 19) else 1
    except ValueError:
        # Not valid, return random 0/1
        return randint(0, 1)


def convert_race_census(race):
    race = race.split()
    r = {
        'American': 'AMERICAN INDIAN/ALASKAN NATIVE',
        'Asian': 'ASIAN / PACIFIC ISLANDER',
        'Native': 'ASIAN / PACIFIC ISLANDER'
    }.get(race[0])

    if race[0] == 'Black':
        r = 'BLACK HISPANIC' if race[-1] == 'Hispanic' else 'BLACK'

    if race[0] == 'White':
        r = 'WHITE HISPANIC' if race[-1] == 'Hispanic' else 'WHITE'

    return r


def convert_county(county):
    return {
        'New York County': 'MANHATTAN',
        'Kings County': 'BROOKLYN',
        'Queens County': 'QUEENS',
        'Bronx County': 'BRONX',
        'Richmond County': 'STATEN ISLAND'
    }.get(county)


def convert_race_shoots(race):
    return {
        'W': 'White',
        'B': 'Black',
        'N': 'Native American',
        'H': 'Hispanic',
        'A': 'Asian',
        'O': 'Others'
    }.get(race)


def convert_date(date):
    year = date.strftime('%Y')
    month= date.strftime('%m')
    return f'{year}-{month}'


def convert_armed(armed):
    return 'armed' if armed != 'unarmed' else 'unarmed'


udf_get_year = F.udf(get_year, IntegerType())
udf_get_month = F.udf(get_month, IntegerType())
udf_parse_locus = F.udf(parse_locus)
udf_parse_daynight = F.udf(parse_daynight)
udf_convert_race_census = F.udf(convert_race_census)
udf_convert_county = F.udf(convert_county)
udf_convert_race_shoots = F.udf(convert_race_shoots)
udf_convert_date_to_datetime = F.udf(lambda x: datetime.strptime(x, '%Y-%m-%d'), DateType())
udf_convert_date_to_string = F.udf(convert_date)
udf_convert_armed = F.udf(convert_armed)


def _map_to_pandas(rdds):
    """ Needs to be here due to pickling issues """
    return [pd.DataFrame(list(rdds))]


def self_toPandas(df, n_partitions=None):
    """
    Returns the contents of `df` as a local `pandas.DataFrame` in a speedy fashion. The DataFrame is
    repartitioned if `n_partitions` is passed.
    :param df:              pyspark.sql.DataFrame
    :param n_partitions:    int or None
    :return:                pandas.DataFrame
    """
    if n_partitions is not None:
        df = df.repartition(n_partitions)
    df_pand = df.rdd.mapPartitions(_map_to_pandas).collect()
    df_pand = pd.concat(df_pand)
    df_pand.columns = df.columns
    return df_pand