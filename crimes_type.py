import datetime

from src.spark import create_session, create_rdd, Filter, create_df
import pyspark.sql.functions as F
from src.database.contracts import nypd_contract as c
import numpy as np
import pandas as pd
import seaborn as sns
import json
import matplotlib.pyplot as plt
from pyspark import StorageLevel
from src.spark.schema import SCHEMA, COLUMNS, OFFENSE_LEVELS

plt.rcParams["figure.figsize"] = [20, 8]


def blank_as_null(x):
    return F.when(F.col(x) != "", F.col(x)).otherwise('ND')


def top_20_crimes(nypd_crimes_df):

    # data cleaning:
    # 1: delete rows without a OFFENSE_DESCRIPTION
    df = nypd_crimes_df.filter(F.length(F.col(c.OFFENSE_DESCRIPTION)) > 0).persist(StorageLevel.MEMORY_AND_DISK)

    # df.show(n=3, truncate=False)

    # crime types
    crime_type_groups = df.groupBy(c.OFFENSE_DESCRIPTION).count().cache()
    crime_type_counts = crime_type_groups.orderBy('count', ascending=False)

    # select the top 20 most frequent crimes and plot the distribution
    counts_crime_pddf = crime_type_counts.toPandas()
    counts_crime_pddf_top20 = counts_crime_pddf[:20]

    print(counts_crime_pddf_top20)
    counts_crime_pddf_top20.plot.barh(x=c.OFFENSE_DESCRIPTION, y='count')
    plt.savefig('top_20_crimes.png')


def crimes_distrib(nypd_crimes_df, out):

    nypd_df = nypd_crimes_df.filter(F.length(F.col(c.DATE)) > 0)

    nypd_df = nypd_df.sort(F.col(c.DATE)).persist(StorageLevel.MEMORY_AND_DISK)

    # min_date, max_date = nypd_df.select(F.min(c.DATE), F.max(c.DATE)).first()
    # print(min_date, max_date)

    timeframing_df = nypd_df.withColumn('date', F.to_date(c.DATE, 'MM/dd/yyyy')) \
        .withColumn('year', F.trunc('date', 'YYYY')).groupby('year').count()
    # timeframing_df.show(n=4, truncate=False)

    timeframing_pddf = timeframing_df.toPandas()

    timeframing_pddf['yearpd'] = timeframing_pddf['year'].apply(lambda x: str(x)[:4])
    timeframing_pddf = timeframing_pddf[timeframing_pddf['count'] > 400000]

    fig, ax = plt.subplots()
    x = timeframing_pddf['yearpd']
    ax.plot(x, timeframing_pddf['count'], label='Crimes')
    ax.set(xlabel='Year - 2009-2019', ylabel='Total records',
           title='Year-on-year crime records')
    ax.grid(b=True, which='both', axis='y')
    ax.legend()
    plt.savefig(out, dpi=300)


def crimes_severity(nypd_df, out):

    # analyze crimes severity over years
    nypd_df = nypd_df.filter(F.length(F.col(c.LEVEL_OFFENSE)) > 0)

    grouped_severity_df = nypd_df.withColumn('date', F.to_date(c.DATE, 'MM/dd/yyyy')) \
        .withColumn('year', F.trunc('date', 'YYYY')).groupby('year', c.LEVEL_OFFENSE).count()

    severity_framing_pddf = grouped_severity_df.toPandas()

    severity_framing_pddf['yearpd'] = severity_framing_pddf['year'].apply(lambda x: int(str(x)[:4]))
    severity_framing_pddf = severity_framing_pddf[(severity_framing_pddf['yearpd'] > 2008)]
    gr_severity_framing_pddf = severity_framing_pddf.groupby(['yearpd', c.LEVEL_OFFENSE]).sum()

    plt.figure()
    gr_severity_framing_pddf['count'].unstack().plot.bar()
    plt.xticks(rotation=0)
    plt.ylabel("Counts")
    plt.xlabel('Crimes severity year-on-year')
    plt.savefig(out, dpi=300)


def main():
    spark = create_session()
    spark.sparkContext.setLogLevel('ERROR')
    sc = spark.sparkContext

    try:

        #nypd_crimes_rdd = create_rdd(spark, COLUMNS)
        nypd_df = create_df(spark, COLUMNS)

        # TOP 20 CRIMES:
        # top_20_crimes(nypd_crimes_rdd)

        # CRIMES distrib over years
        crimes_distrib(nypd_df, 'crimes_counts.png')

        # CRIMES SEVERITY distrib
        crimes_severity(nypd_df, 'crimes_severity.png')


    except Exception as e:
        print(e)
        sc.stop()
        spark.stop()


if __name__ == '__main__':
    main()
