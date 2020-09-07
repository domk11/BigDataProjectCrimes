from src.spark import create_session, create_df
import pyspark.sql.functions as F
from src.database.contracts import nypd_contract as c
import matplotlib.pyplot as plt
from src.spark.schema import COLUMNS

plt.rcParams["figure.figsize"] = [20, 8]


def top_20_crimes(nypd_crimes_df):

    # data cleaning:
    # 1: delete rows without a OFFENSE_DESCRIPTION
    df = nypd_crimes_df.filter(F.length(F.col(c.OFFENSE_DESCRIPTION)) > 0)

    # df.show(n=3, truncate=False)

    # crime types
    crime_type_groups = df.groupBy(c.OFFENSE_DESCRIPTION).count()
    crime_type_counts = crime_type_groups.orderBy('count', ascending=False)

    # select the top 20 most frequent crimes and plot the distribution
    counts_crime_pddf = crime_type_counts.toPandas()
    counts_crime_pddf_top20 = counts_crime_pddf[:20]

    print(counts_crime_pddf_top20)
    counts_crime_pddf_top20.plot.barh(x=c.OFFENSE_DESCRIPTION, y='count')
    plt.savefig('top_20_crimes.png')


def crimes_distrib(nypd_crimes_df, out):

    nypd_df = nypd_crimes_df.filter(F.length(F.col(c.DATE)) > 0)

    nypd_df = nypd_df.sort(F.col(c.DATE))

    # min_date, max_date = nypd_df.select(F.min(c.DATE), F.max(c.DATE)).first()
    # print(min_date, max_date)

    timeframing_df = nypd_df.withColumn('date', F.to_date(c.DATE, 'MM/dd/yyyy')) \
        .withColumn('year', F.trunc('date', 'YYYY')).groupby('year').count()
    # timeframing_df.show(n=4, truncate=False)

    timeframing_pddf = timeframing_df.toPandas()

    timeframing_pddf['yearpd'] = timeframing_pddf['year'].apply(lambda x: int(str(x)[:4]))
    timeframing_pddf = timeframing_pddf[(timeframing_pddf['yearpd'] > 2008)]
    timeframing_pddf.sort_values(by='yearpd', inplace=True)

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


def crimes_severity_by_district(nypd_df, out):
    # clean dataset from empty BOROUGH
    df = nypd_df.filter(F.length(F.col(c.BOROUGH)) > 0)

    # list all boroughs of NY
    boroughs_list = [v[c.BOROUGH] for v in nypd_df.select(c.BOROUGH).distinct().collect()]
    print(boroughs_list)

    # select only last year
    crimes_df = df.withColumn('date', F.to_date(c.DATE, 'MM/dd/yyyy'))\
                  .withColumn('year', F.trunc('date', 'YYYY'))\
                  .where(F.col('year') == F.lit('2019-01-01'))\
                  .select([c.BOROUGH, c.LEVEL_OFFENSE])

    grouped_crimes_df = crimes_df.groupby([c.BOROUGH, c.LEVEL_OFFENSE]).count()
    grouped_crimes_df.show(n=10, truncate=False)

    crimes_pddf = grouped_crimes_df.toPandas()
    gr_severity_crimes_pddf = crimes_pddf.groupby([c.BOROUGH, c.LEVEL_OFFENSE]).sum()

    plt.figure()
    ax = gr_severity_crimes_pddf.unstack().plot(kind='bar', stacked=True)
    ax.legend(crimes_pddf[c.LEVEL_OFFENSE].unique())
    plt.xticks(rotation=0)
    plt.savefig(out, dpi=300)


def dom(nypd_df):
    
    crime_race_groups = nypd_df.withColumn(
                                c.RACE, F.when(F.col(c.RACE) == '', 'UNKNOWN').otherwise(F.col(c.RACE))
                                ).groupBy(c.RACE).count()

    crime_race_counts = crime_race_groups.orderBy('count', ascending=False)

    counts_race_pddf = crime_race_counts.toPandas()
    counts_race_pddf.set_index(c.RACE, inplace=True)
    counts_race_pddf_top8 = counts_race_pddf[:8]

    print(counts_race_pddf_top8)
    plt.figure()
    counts_race_pddf_top8.plot.pie(y='count')
    plt.savefig('pandas2.png')


def main():
    spark = create_session()
    spark.sparkContext.setLogLevel('ERROR')
    sc = spark.sparkContext

    try:

        #nypd_crimes_rdd = create_rdd(spark, COLUMNS)
        nypd_df = create_df(spark, COLUMNS).cache()

        # TOP 20 CRIMES:
        # top_20_crimes(nypd_crimes_rdd)

        # CRIMES distrib over years
        # crimes_distrib(nypd_df, 'crimes_counts.png')

        # CRIMES SEVERITY distrib
        # crimes_severity(nypd_df, 'crimes_severity.png')

        # crimes_severity_by_district(nypd_df, 'crimes_borough_severity.png')

        dom(nypd_df)


    except Exception as e:
        print(e)
        sc.stop()
        spark.stop()


if __name__ == '__main__':
    main()
