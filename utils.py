import pyspark.sql.functions as F


def get_year(year_str):
    # expects format: 2009-01-01
    return str(year_str)[:4]


def parse_locus(locus_str):
    locus_str = locus_str.upper()
    if locus_str != 'INSIDE':
        return 'OUTSIDE'
    return 'INSIDE'


udf_get_year = F.udf(get_year)
udf_parse_locus = F.udf(parse_locus)