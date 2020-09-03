from src.spark import create_session, create_rdd, Filter
import pyspark.sql.functions as F
from src.database.contracts import nypd_contract as c
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark import StorageLevel
from src.spark.schema import SCHEMA

def blank_as_null(x):
    return F.when(F.col(x) != "", F.col(x)).otherwise('ND')


def main():
    spark = create_session()
    sc = spark.sparkContext

    try:
        columns = [c.ID, c.DATE, c.TIME, c.PRECINCT, c.OFFENSE_CODE, c.OFFENSE_DESCRIPTION, c.CRIME_OUTCOME,
                   c.LEVEL_OFFENSE, c.BOROUGH, c.LATITUDE, c.LONGITUDE, c.AGE, c.RACE, c.SEX]

        nypd_crimes_rdd = create_rdd(spark, columns)
        nypd_df = nypd_crimes_rdd.toDF()

        # cache this because will be read many times
        f = Filter(nypd_crimes_rdd)

        # data cleaning:
        # 1: delete rows without a OFFENSE_DESCRIPTION
        df = nypd_crimes_rdd.filter(lambda x: [x[y] != '' for y in columns]).toDF().persist(StorageLevel.MEMORY_AND_DISK)
        # df.show(n=3, truncate=False)

        # crime types
        crime_type_groups = df.groupBy(c.OFFENSE_DESCRIPTION).count().cache()
        crime_type_counts = crime_type_groups.orderBy('count', ascending=False)
        #

        counts_crime_pddf = crime_type_counts.toPandas()
        counts_crime_pddf_top20 = counts_crime_pddf[:20]
        # mask = counts_crime_pddf['count'] / counts_crime_pddf['count'].sum() < 0.10
        # categories = np.where(mask, 'OTHERS', counts_crime_pddf[c.OFFENSE_DESCRIPTION])
        # d = counts_crime_pddf.groupby(categories)['count'].sum()

        print(counts_crime_pddf_top20)
        plt.rcParams["figure.figsize"] = [20, 8]
        counts_crime_pddf_top20.plot.barh(x=c.OFFENSE_DESCRIPTION, y='count')
        plt.savefig('pandas.png')

    except Exception as e:
        print(e)
        sc.stop()
        spark.stop()


if __name__ == '__main__':
    main()
