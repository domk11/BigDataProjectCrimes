import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
import pymongo_spark
pymongo_spark.activate()


def main():
    conf = SparkConf().setAppName('pyspark test')
    sc = SparkContext(conf=conf)
    mongo_rdd = sc.mongoRDD('mongodb://localhost:27017/datascience.nypd', {'mongo.splitter.class': 'com.mongodb.hadoop.splitter.StandaloneMongoSplitter'})
    print(mongo_rdd.first())
    sc.stop()

if __name__ == '__main__':
    main()
