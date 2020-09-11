import pandas as pd
import squarify
import folium
import numpy as np
import seaborn as sns
from src.spark import create_session, create_df
import pyspark.sql.functions as F
from src.database.contracts import fatal_contract as c
import matplotlib.pyplot as plt
from src.spark.schemafatal import COLUMNS

plt.rcParams["figure.figsize"] = [20, 8]

def cross_age_races(data):
    plt.figure()
    data1 = data.filter(F.length(F.col(c.AGE)) > 0)
    data2 = data1.filter(F.length(F.col(c.RACE)) > 0)
    data2 = data2.groupby([c.AGE, c.RACE]).count().toPandas()
    df = pd.crosstab(data2.SUSP_AGE_GROUP, data2.SUSP_RACE )
    color = plt.cm.gist_rainbow(np.linspace(0, 1, 10))

    df.div(df.sum(1).astype(float), axis = 0).plot.bar(stacked = True, color = color, figsize = (18, 12))
    plt.title('District vs Category of Crime', fontweight = 30, fontsize = 20)

    plt.xticks(rotation = 90)
    plt.savefig('crosskill.png')

def plot_races(data):

    plt.figure()
    data = data.groupBy(c.RACE).count()
    races_count = data.orderBy('count', ascending=False).toPandas()

    races_count.set_index(c.RACE, inplace=True)

    races_count.plot.pie(y='count')
    plt.savefig('racesfatal.png')

def main():
    spark = create_session()
    spark.sparkContext.setLogLevel('ERROR')
    sc = spark.sparkContext

    try:

        #nypd_crimes_rdd = create_rdd(spark, COLUMNS)
        killdf = create_df(spark, COLUMNS).cache()
        killdf = killdf.filter(F.length(F.col(c.RACE)) > 0)
        #pddf = killdf.toPandas()

        plot_races(killdf)



    except Exception as e:
        print(e)
        sc.stop()
        spark.stop()


if __name__ == '__main__':
    main()