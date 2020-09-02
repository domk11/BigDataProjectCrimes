from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

from src.database.contracts import nypd_contract as c


SCHEMA = StructType([StructField(c.ID, StringType()),
                     StructField(c.DATE, StringType()),
                     StructField(c.TIME, StringType()),
                     StructField(c.PRECINCT, IntegerType()),
                     StructField(c.OFFENSE_CODE, StringType()),
                     StructField(c.OFFENSE_DESCRIPTION, StringType()),
                     StructField(c.CRIME_OUTCOME, StringType()),
                     StructField(c.LEVEL_OFFENSE, StringType()),
                     StructField(c.BOROUGH, StringType()),
                     StructField(c.LATITUDE, FloatType()),
                     StructField(c.LONGITUDE, FloatType()),
                     StructField(c.AGE, StringType()),
                     StructField(c.RACE, StringType()),
                     StructField(c.SEX, StringType())
                     ])
