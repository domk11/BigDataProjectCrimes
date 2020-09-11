from pyspark.sql.types import *

from src.database.contracts import fatal_contract as c


SCHEMA = StructType([StructField(c.ID, StringType()),
                     StructField(c.AGE, StringType()),
                     StructField(c.RACE, StringType()),
                     StructField(c.SEX, StringType()),
                     StructField(c.DEATH_DATE, StringType())
                     ])


COLUMNS = [c.ID, c.AGE, c.RACE, c.SEX, c.DEATH_DATE]


#OFFENSE_LEVELS = ['FELONY', 'MISDEMEANOR', 'VIOLATION']