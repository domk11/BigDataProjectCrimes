import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType, StructField, StringType
import pymongo_spark
pymongo_spark.activate()

from lib.database.contracts import nypd_contract as c


def main():
    conf = SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.3.2").setAppName('pyspark test')
    conf.set("spark.mongodb.input.uri", "mongodb://localhost:27017/datascience.nypd")
    conf.set("spark.mongodb.output.uri", "mongodb://localhost:27017/datascience.nypd")
    sc = SparkContext(conf=conf)

    spark = SparkSession(sc).builder.config('spark.driver.extraClassPath', '/home/marco/jars/*')

    mongo_rdd = sc.mongoRDD('mongodb://localhost:27017/datascience.nypd',
                            {'mongo.splitter.class':'com.mongodb.hadoop.splitter.StandaloneMongoSplitter'})


    males_rdd = mongo_rdd.filter(lambda x: (x[c.SEX] == 'M') & (x[c.AGE] == '<18'))
    print(males_rdd.first())
    print(males_rdd.take(10))

    schema = StructType([StructField(c.ID, StringType()),
                         StructField(c.DATE, StringType()),
                         StructField(c.TIME, StringType()),
                         StructField(c.PRECINCT, StringType()),
                         StructField(c.OFFENSE_CODE, StringType()),
                         StructField(c.OFFENSE_DESCRIPTION, StringType()),
                         StructField(c.CRIME_OUTCOME, StringType()),
                         StructField(c.LEVEL_OFFENSE, StringType()),
                         StructField(c.BOROUGH, StringType()),
                         StructField(c.LATITUDE, StringType()),
                         StructField(c.LONGITUDE, StringType()),
                         StructField(c.AGE, StringType()),
                         StructField(c.RACE, StringType()),
                         StructField(c.SEX, StringType())
                         ])

    males_rdd = males_rdd.toDF(schema=schema)
    males_rdd.printSchema()

    sqlContext = SQLContext(sc)

    df = sqlContext.read.schema(schema).load()
    print(df.first())
    df.registerTempTable("mycollection")
    result_data = sqlContext.sql("SELECT * from mycollection limit 10")
    result_data.show()

    sc.stop()


if __name__ == '__main__':
    main()
