import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
import pymongo_spark
pymongo_spark.activate()


def main():
    conf = SparkConf().setAppName('pyspark test')
    sc = SparkContext(conf=conf)
    rdd = sc.mongoRDD('mongodb://localhost:27017/data_science')


if __name__ == '__main__':
    main()