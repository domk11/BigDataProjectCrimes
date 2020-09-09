import pandas as pd
import squarify
import folium
import numpy as np
import seaborn as sns
from src.spark import create_session, create_df
import pyspark.sql.functions as F
from src.database.contracts import nypd_contract as c
import matplotlib.pyplot as plt
from src.spark.schema import COLUMNS

plt.rcParams["figure.figsize"] = [20, 8]

def crime_race(nypd_df):

    crime_race_groups = nypd_df.withColumn(
        c.RACE, F.when(F.col(c.RACE) == '', 'UNKNOWN').otherwise(F.col(c.RACE))
    ).groupBy(c.RACE).count()

    crime_race_counts = crime_race_groups.orderBy('count', ascending=False)

    counts_race_pddf = crime_race_counts.toPandas()
    counts_race_pddf.set_index(c.RACE, inplace=True)
    counts_race_pddf_top8 = counts_race_pddf[:8]

    #print(counts_race_pddf_top8)
    plt.figure()
    counts_race_pddf_top8.plot.pie(y='count')
    plt.savefig('pandas2.png')

def crime_age(data):
    crime_age_groups = data.withColumn(
        c.AGE, F.when(F.col(c.AGE) == '', 'UNKNOWN').otherwise(F.col(c.AGE))
    ).groupBy(c.AGE).count()

    crime_age_counts = crime_age_groups.orderBy('count', ascending=False)

    pddf = crime_age_counts.toPandas()
    pddf.set_index(c.AGE, inplace=True)
    plt.figure()
    pddf.plot.pie(y='count')
    plt.savefig('agegroup.png')

def scat_age_race(data):

    plt.figure()
    sns.catplot(x=datapd.SUSP_AGE_GROUP, y='count', hue=datapd.SUSP_RACE ,kind="bar", data=datapd)
    plt.savefig('scat.png')

def cross_district_races(data):
    plt.figure()
    data1 = data.filter(F.length(F.col(c.BOROUGH)) > 0)
    data2 = data1.filter(F.length(F.col(c.RACE)) > 0).toPandas()
    df = pd.crosstab(data2.BORO_NM, data2.SUSP_RACE )
    color = plt.cm.gist_rainbow(np.linspace(0, 1, 10))

    df.div(df.sum(1).astype(float), axis = 0).plot.bar(stacked = True, color = color, figsize = (18, 12))
    plt.title('District vs Category of Crime', fontweight = 30, fontsize = 20)

    plt.xticks(rotation = 90)
    plt.savefig('crossrace.png')

def cross_district_crimes(data):
    plt.figure()
    data1 = data.filter(F.length(F.col(c.BOROUGH)) > 0)
    data2 = data1.filter(F.length(F.col(c.OFFENSE_DESCRIPTION)) > 0).toPandas()
    df = pd.crosstab(data2.BORO_NM, data2.OFNS_DESC )
    color = plt.cm.gist_rainbow(np.linspace(0, 1, 10))

    df.div(df.sum(1).astype(float), axis = 0).plot.bar(stacked = True, color = color, figsize = (18, 12))
    plt.title('District vs Category of Crime', fontweight = 30, fontsize = 20)

    plt.xticks(rotation = 90)
    plt.savefig('crossi.png')

def main():
    spark = create_session()
    spark.sparkContext.setLogLevel('ERROR')
    sc = spark.sparkContext

    try:

        #nypd_crimes_rdd = create_rdd(spark, COLUMNS)
        nypd_df = create_df(spark, COLUMNS).cache()
        cross_district_crimes(nypd_df)
        cross_district_races(nypd_df)
        #crime_age(nypd_df)
        #scat_age_race(nypd_df)
        #crime_race(nypd_df)


    except Exception as e:
        print(e)
        sc.stop()
        spark.stop()


if __name__ == '__main__':
    main()

'''

        crime_type_groups = df.groupBy(c.OFFENSE_DESCRIPTION).count()
        crime_type_counts = crime_type_groups.orderBy('count', ascending=False)
        #
       # df_cleaned = df.replace('', np.nan)
       # df_cleaned = df_cleaned.dropna()
        crime_race_groups = df.groupBy(c.RACE).count()
        crime_race_counts = crime_race_groups.orderBy('count', ascending=False)

        timedf1 = df.groupBy(c.TIME).count()
        timedf = timedf1.orderBy('count', ascending=False).toPandas()
        pdfr1 = df.groupBy(c.PRECINCT).count()
        pdfr = pdfr1.orderBy('count', ascending=False).toPandas()
        pdbor1 = df.groupBy(c.BOROUGH).count()
        pdbor = pdbor1.orderBy('count', ascending=False).toPandas()

        counts_crime_pddf = crime_type_counts.toPandas()
        counts_crime_pddf_clean = counts_crime_pddf.replace('', 'UNKNOWN')
        counts_crime_pddf_top20 = counts_crime_pddf_clean[:20]
        # mask = counts_crime_pddf['count'] / counts_crime_pddf['count'].sum() < 0.10
        # categories = np.where(mask, 'OTHERS', counts_crime_pddf[c.OFFENSE_DESCRIPTION])
        # d = counts_crime_pddf.groupby(categories)['count'].sum()
        counts_race_pddf = crime_race_counts.toPandas()

        counts_race_pddf_clean = counts_race_pddf.replace('', 'UNKNOWN')
        counts_race_pddf_top8 = counts_race_pddf_clean[:8]

        print(timedf.head(10))

'''

def regions(data):
    plt.figure()
    plt.rcParams['figure.figsize'] = (20, 9)
    plt.style.use('seaborn')

    color = plt.cm.spring(np.linspace(0, 1, 15))
    data.ADDR_PCT_CD.value_counts().plot.bar(color = color, figsize = (15, 10))

    plt.title('District with Most Crime',fontsize = 30)

    plt.xticks(rotation = 90)

    plt.savefig('regions.png')

def races(data):
    plt.figure()
    y = data.SUSP_RACE.value_counts()

    plt.rcParams['figure.figsize'] = (15, 15)
    plt.style.use('fivethirtyeight')

    color = plt.cm.magma(np.linspace(0, 1, 15))
    squarify.plot(sizes = y.values, label = y.index, alpha=.8, color = color)
    plt.title('Tree Map for races', fontsize = 20)

    plt.axis('off')

    plt.savefig('pandas2.png')



def crime(data):
    plt.figure()
    plt.rcParams['figure.figsize'] = (20, 9)
    plt.style.use('dark_background')

    sns.countplot(data.OFNS_DESC, palette = 'gnuplot')

    plt.title('Races', fontweight = 30, fontsize = 20)
    plt.xticks(rotation = 90)

    #plt.rcParams["figure.figsize"] = [8, 8]
    #counts_race_pddf_top8.plot.pie(y='count')

    plt.savefig('crimes.png')

def addrss(data):
    # Regions with count of crimes
    plt.figure()
    plt.rcParams['figure.figsize'] = (20, 9)
    plt.style.use('seaborn')

    color = plt.cm.ocean(np.linspace(0, 1, 15))
    data.BORO_NM.value_counts().head(15).plot.bar(color = color, figsize = (15, 10))

    plt.title('Top 15 Regions in Crime',fontsize = 20)

    plt.xticks(rotation = 90)

    plt.savefig('addr.png')





if __name__ == '__main__':
    main()
