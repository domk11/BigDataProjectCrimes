import datetime

from src.spark import create_session, create_rdd, Filter
import pyspark.sql.functions as F
from src.database.contracts import nypd_contract as c
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark import StorageLevel
from src.spark.schema import SCHEMA, COLUMNS


def blank_as_null(x):
    return F.when(F.col(x) != "", F.col(x)).otherwise('ND')


def top_20_crimes(nypd_crimes_rdd):

    # data cleaning:
    # 1: delete rows without a OFFENSE_DESCRIPTION
    df = nypd_crimes_rdd.filter(lambda x: [x[y] != '' for y in COLUMNS]).toDF().persist(StorageLevel.MEMORY_AND_DISK)
    # df.show(n=3, truncate=False)

    # crime types
    crime_type_groups = df.groupBy(c.OFFENSE_DESCRIPTION).count().cache()
    crime_type_counts = crime_type_groups.orderBy('count', ascending=False)

    # select the top 20 most frequent crimes and plot the distribution
    counts_crime_pddf = crime_type_counts.toPandas()
    counts_crime_pddf_top20 = counts_crime_pddf[:20]

    print(counts_crime_pddf_top20)
    plt.rcParams["figure.figsize"] = [20, 8]
    counts_crime_pddf_top20.plot.barh(x=c.OFFENSE_DESCRIPTION, y='count')
    plt.savefig('top_20_crimes.png')


def main():
    spark = create_session()
    sc = spark.sparkContext

    try:

        nypd_crimes_rdd = create_rdd(spark, COLUMNS, limit=5000)
        nypd_df = nypd_crimes_rdd.toDF().persist(StorageLevel.MEMORY_AND_DISK)


        # TOP 20 CRIMES:
        # top_20_crimes(nypd_crimes_rdd)

        # TIME FRAMING:
        nypd_df.select(F.min(c.DATE).alias('first_record_date'), F.max(c.DATE).alias('latest_record_date')).show(truncate=False)
        min_date, max_date = nypd_df.select(F.min(c.DATE), F.max(c.DATE)).first()
        print(min_date, max_date)

        timeframing_df = nypd_df.withColumn('date_time', F.to_timestamp(c.DATE, 'MM/dd/yyyy'))\
                                .withColumn('year', F.trunc('date_time', 'YYYY'))
        timeframing_df.select([c.DATE, 'year']).show(n=2, truncate=False)
        timeframing_pddf = timeframing_df.toPandas()
        timeframing_pddf['yearpd'] = timeframing_pddf['year'].apply(lambda dt: datetime.datetime.strftime(pd.Timestamp(dt), '%Y'))
        print(timeframing_pddf.head())

        timeframing_pddf_year_grouped = timeframing_pddf.groupby('yearpd').size().reset_index(name='count')
        print(timeframing_pddf_year_grouped.head())

        fig, ax = plt.subplots()
        ax.plot(timeframing_pddf_year_grouped['yearpd'], timeframing_pddf_year_grouped['count'], label='Crimes')

        ax.set(xlabel='Year - 2001-2017', ylabel='Total records',
               title='Year-on-year crime records')
        ax.grid(b=True, which='both', axis='y')
        ax.legend()
        plt.savefig('crimes.png')
        # cache this because will be read many times
        f = Filter(nypd_crimes_rdd)




    except Exception as e:
        print(e)
        sc.stop()
        spark.stop()


if __name__ == '__main__':
    main()
