import pyspark.sql.functions as F
from shapely.geometry import Point, Polygon
from src.database.contracts import nypd_contract as c


def get_year(year_str):
    # expects format: 2009-01-01
    return str(year_str)[:4]


def parse_locus(locus_str):
    locus_str = locus_str.upper()
    if locus_str != 'INSIDE':
        return 'OUTSIDE'
    return 'INSIDE'


def parse_coordinates(lat, long):
    return Point(float(lat), float(long))


def parse_daynight(time):
    # expects TIME like 15:00:00, returns:
    # 0 --> day [06 - 18]
    # 1 --> night [19 - 05]
    hour = int(time[:2])
    if (hour > 5) & (hour < 19):
        return 0
    return 1

udf_get_year = F.udf(get_year)
udf_parse_locus = F.udf(parse_locus)
udf_parse_coordinates = F.udf(parse_coordinates)
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



