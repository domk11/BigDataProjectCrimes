import findspark
findspark.init()

from pyspark import SparkConf
from pyspark.sql import SparkSession

from .schema import SCHEMA, COLUMNS, OFFENSE_LEVELS
from .filter import Filter
from .SparkCensus import SparkCensus
from .SparkShoots import SparkShoots
from .spark_politics import SparkPolitics

MONGO_URI = 'mongodb://localhost:27017/datascience'


def create_session(collection):
    conf = SparkConf() \
        .set('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.4.1') \
        .set('spark.mongodb.input.uri', f'{MONGO_URI}.{collection}') \
        .set('spark.mongodb.output.uri', f'{MONGO_URI}.{collection}') \
        .setAppName('big_data')

    return SparkSession.builder \
        .master('local') \
        .config(conf=conf) \
        .config('spark.driver.memory', '8g') \
        .config('spark.executor.memory', '4g') \
        .getOrCreate()


def create_df(spark):
    return spark.read.format('mongo').option('inferSchema', 'false').option('sampleSize', 50000).load()


def create_rdd(spark, columns=None):
    return spark.read.format('mongo').option('inferSchema', 'false').option('sampleSize', 50000).load().select(columns).rdd
