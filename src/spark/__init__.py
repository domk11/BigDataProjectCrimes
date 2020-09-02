import findspark
findspark.init()

from pyspark import SparkConf
from pyspark.sql import SparkSession

from .schema import SCHEMA
from .filter import Filter


MONGO_URI = 'mongodb://localhost:27017/datascience.nypd'


def create_session():
    conf = SparkConf() \
        .set('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.4.1') \
        .set('spark.mongodb.input.uri', MONGO_URI) \
        .set('spark.mongodb.output.uri', MONGO_URI) \
        .setAppName('big_data')

    return SparkSession.builder \
        .master('local') \
        .config(conf=conf) \
        .getOrCreate()


def create_rdd(spark):
    return spark.read.format('mongo').load().rdd