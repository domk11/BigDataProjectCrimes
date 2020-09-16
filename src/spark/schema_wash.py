from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DoubleType

from src.database.contracts import wash_contract as c


SCHEMA = StructType([StructField(c.ID, StringType()),
                     StructField(c.DATE, DateType()),
                     StructField(c.AGE, StringType()),
                     StructField(c.RACE, StringType()),
                     StructField(c.SEX, StringType())])


COLUMNS = [c.ID, c.DATE, c.AGE, c.RACE, c.SEX]
