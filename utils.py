import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import *
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




