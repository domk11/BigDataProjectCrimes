import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType, StructField, StringType
import pymongo_spark
pymongo_spark.activate()

ID = '_id'
DATE = 'CMPLNT_FR_DT'
TIME = 'CMPLNT_FR_TM'
PRECINCT = 'ADDR_PCT_CD'
OFFENSE_CODE = 'KY_CD'
OFFENSE_DESCRIPTION = 'OFNS_DESC'
CRIME_OUTCOME = 'CRM_ATPT_CPTD_CD'
LEVEL_OFFENSE = 'LAW_CAT_CD'
BOROUGH = 'BORO_NM'
LATITUDE = 'Latitude'
LONGITUDE = 'Longitude'
AGE = 'SUSP_AGE_GROUP'
RACE = 'SUSP_RACE'
SEX = 'SUSP_SEX'


def project(doc):
    return {ID: str(doc[ID]),
            DATE: str(doc[DATE]),
            TIME: str(doc[TIME]),
            PRECINCT: str(doc[PRECINCT]),
            OFFENSE_CODE: str(doc[OFFENSE_CODE]),
            OFFENSE_DESCRIPTION: str(doc[OFFENSE_DESCRIPTION]),
            CRIME_OUTCOME: str(doc[CRIME_OUTCOME]),
            LEVEL_OFFENSE: str(doc[LEVEL_OFFENSE]),
            BOROUGH: str(doc[BOROUGH]),
            LATITUDE: str(doc[LATITUDE]),
            LONGITUDE: str(doc[LONGITUDE]),
            AGE: str(doc[AGE]),
            RACE: str(doc[RACE]),
            SEX: str(doc[SEX])
            }

def main():
    conf = SparkConf().set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat").setAppName('pyspark test')
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    mongo_rdd = sc.mongoRDD('mongodb://localhost:27017/datascience.nypd',{'mongo.splitter.class':'com.mongodb.hadoop.splitter.StandaloneMongoSplitter'})
    
    males_rdd = mongo_rdd.filter(lambda x: (x[SEX] == 'M') & (x[AGE] == '<18'))
    print(males_rdd.first())
    print(males_rdd.take(10))

    schema = StructType([StructField(ID, StringType()),
                         StructField(DATE, StringType()),
                         StructField(TIME, StringType()),
                         StructField(PRECINCT, StringType()),
                         StructField(OFFENSE_CODE, StringType()),
                         StructField(OFFENSE_DESCRIPTION, StringType()),
                         StructField(CRIME_OUTCOME, StringType()),
                         StructField(LEVEL_OFFENSE, StringType()),
                         StructField(BOROUGH, StringType()),
                         StructField(LATITUDE, StringType()),
                         StructField(LONGITUDE, StringType()),
                         StructField(AGE, StringType()),
                         StructField(RACE, StringType()),
                         StructField(SEX, StringType())
                         ])

    males_rdd = males_rdd.toDF(schema=schema)
    males_rdd.printSchema()
    sc.stop()


if __name__ == '__main__':
    main()
