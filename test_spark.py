from src.spark import create_session, create_rdd, Filter, COLUMNS
from src.database.contracts import nypd_contract as c


def main():
    spark = create_session(c.FILTERED_COLLECTION)

    try:
        mongo_rdd = create_rdd(spark, COLUMNS)
        print(mongo_rdd.first())

        f = Filter(mongo_rdd)

        males_rdd = f.filter_sex('M')
        print(type(males_rdd.first()))
        print(males_rdd.first())
        print(males_rdd.take(10))

        nypd_crimes_df = spark.read.format('mongo').load()
        nypd_crimes_df.show()
        nypd_crimes_df.printSchema()

        # nypd_crimes_df = spark.createDataFrame(nypd_collection, schema=schema)
        nypd_crimes_df.select(c.OFFENSE_DESCRIPTION).distinct().show()

        # count how many crimes for types
        nypd_crimes_df.groupBy(c.OFFENSE_DESCRIPTION).count().show()
    except:
        spark.stop()


if __name__ == '__main__':
    main()
