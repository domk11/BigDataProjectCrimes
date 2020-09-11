import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from src.database.contracts import nypd_contract as c
from random import randint

def get_year(year_str):
    # expects format: 2009-01-01
    return int(str(year_str)[:4])


def parse_locus(locus_str):
    locus_str = locus_str.upper()
    if locus_str != 'INSIDE':
        return 'OUTSIDE'
    return 'INSIDE'


def parse_daynight(time):
    # expects TIME like 15:00:00, returns:
    # 0 --> day [06 - 18]
    # 1 --> night [19 - 05]
    try:
        hour = int(time[:2])
        if (hour > 5) & (hour < 19):
            return 0
        return 1
    except ValueError:
        # Not valid, return random 0/1
        return randint(0, 1)

udf_get_year = F.udf(get_year, IntegerType())
udf_parse_locus = F.udf(parse_locus)
udf_parse_daynight = F.udf(parse_daynight)

def fix_date_nypd(nypd_df, timeframing=None):
    # timeframing if not none expects an array like: [lower bound, upper bound]
    # for example: timeframing = [2009, 2019] to get record from the last 10 years
    df = nypd_df.filter(
        (F.length(F.col(c.DATE)) > 0)
                        )

    df = df.withColumn('date', F.to_date(c.DATE, 'MM/dd/yyyy')) \
           .withColumn('year', F.trunc('date', 'YYYY')) \
           .withColumn('yearpd', udf_get_year('year')) \
           .select('*')

    if timeframing:

        if len(timeframing) != 2:
            return -1

        df = df.where(
            (F.col('yearpd') >= F.lit(str(timeframing[0]))) & (F.col('yearpd') <= F.lit(str(timeframing[1])))
        ).select('*')

    return df



