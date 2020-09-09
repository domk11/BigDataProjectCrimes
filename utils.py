import pyspark.sql.functions as F
from shapely.geometry import Point, Polygon


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


udf_get_year = F.udf(get_year)
udf_parse_locus = F.udf(parse_locus)
udf_parse_coordinates = F.udf(parse_coordinates)