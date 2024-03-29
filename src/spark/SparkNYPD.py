import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression
import pyspark.sql.functions as F
from pyspark import StorageLevel
from pyspark.sql.types import *

from src.database.contracts import nypd_contract as c
from utils import *


plt.rcParams['figure.figsize'] = (12, 8)


class SparkNYPD:

    nypd_df = None

    def __init__(self, nypd_df):
        self.nypd_df = nypd_df
        self._preprocess()

    def _preprocess(self):
        self.nypd_df = self.nypd_df.withColumn('date', F.to_date(c.DATE, 'MM/dd/yyyy')) \
                                   .withColumn('yearpd', udf_get_year('date')) \
                                   .withColumn('day_night', udf_parse_daynight(c.TIME).cast('int')) \
                                   .withColumn(c.RACE, F.when(F.col(c.RACE) == '', 'UNKNOWN').otherwise(F.col(c.RACE))) \
                                   .withColumn(c.AGE, F.when(F.col(c.AGE) == '', 'UNKNOWN').otherwise(F.col(c.AGE)))

        # trigger the cache
        self.nypd_df.persist(StorageLevel.MEMORY_AND_DISK).count()

    def _save_csv(self, df, csv_out):
        #df.coalesce(1).write.option('header', 'true').option('sep', ',').mode('overwrite').csv(csv_out)
        df.to_csv(csv_out)

    def crimes_trend(self, df=None, img_out=None, csv_out=None, cache=False):
        nypd_df = self.nypd_df

        if df:
            nypd_df = df

        if cache:
            nypd_df = nypd_df.persist()

        # sort by date
        nypd_df = nypd_df.sort(F.col(c.DATE))

        # min_date, max_date = nypd_df.select(F.min(c.DATE), F.max(c.DATE)).first()
        # print(min_date, max_date)

        crimes_df = nypd_df.groupby('yearpd').count().orderBy('yearpd')

        pddf = crimes_df.toPandas()

        X = pddf['yearpd'].values.reshape(-1, 1)
        Y = pddf['count'].values.reshape(-1, 1)

        linear_regressor = LinearRegression()  # create object for the class
        linear_regressor.fit(X, Y)  # perform linear regression
        Y_pred = linear_regressor.predict(X)  # make predictions

        print(pddf)

        if csv_out:
            self._save_csv(pddf, csv_out)

        if img_out:
            fig, ax = plt.subplots(figsize=(12, 8))
            ax.plot(X, Y, label='Crimes')
            ax.plot(X, Y_pred, '--', label='Trend')
            ax.set(xlabel='Year - 2009-2019',
                   ylabel='Total records',
                   title='Year-on-year crime records')
            ax.grid(b=True, which='both', axis='y')
            ax.legend()
            plt.savefig(img_out)

        return crimes_df

    def crimes_top(self, df=None, n=20, img_out=None, csv_out=None, cache=False):
        nypd_df = self.nypd_df

        if df:
            nypd_df = df

        if cache:
            nypd_df = nypd_df.persist()

        # data cleaning:
        # filter rows without a OFFENSE_DESCRIPTION
        df = nypd_df.filter(F.length(F.col(c.OFFENSE_DESCRIPTION)) > 0)

        # crime types
        crime_type_groups = df.groupBy(c.OFFENSE_DESCRIPTION).count()
        crime_type_counts = crime_type_groups.orderBy('count', ascending=False)

        # select the top N most frequent crimes and plot the distribution
        counts_crime_pddf = crime_type_counts.toPandas()
        counts_crime_pddf_top_N = counts_crime_pddf[:n]

        print(counts_crime_pddf_top_N)

        if img_out:
            plt.figure(figsize=(12, 8))
            counts_crime_pddf_top_N.plot.barh(x=c.OFFENSE_DESCRIPTION, y='count')
            plt.savefig(img_out)

        if csv_out:
            self._save_csv(counts_crime_pddf_top_N, csv_out)

        return crime_type_counts

    def crimes_severity(self, df=None, img_out=None, csv_out=None, cache=False):
        nypd_df = self.nypd_df

        if df:
            nypd_df = df

        if cache:
            nypd_df = nypd_df.persist()

        # analyze crimes severity over years
        nypd_df = nypd_df.filter(F.length(F.col(c.LEVEL_OFFENSE)) > 0)

        grouped_severity_df = nypd_df.groupby('yearpd', c.LEVEL_OFFENSE).count()

        severity_framing_pddf = grouped_severity_df.toPandas()
        grouped_severity_df_pddf = severity_framing_pddf.groupby(by=['yearpd', c.LEVEL_OFFENSE]).sum()

        print(grouped_severity_df_pddf)

        if img_out:
            plt.figure()
            grouped_severity_df_pddf['count'].unstack().plot.bar()
            plt.xticks(rotation=0)
            plt.ylabel('Counts')
            plt.xlabel('Crimes severity year-on-year')
            plt.savefig(img_out)

        if csv_out:
            self._save_csv(grouped_severity_df_pddf, csv_out)

        return grouped_severity_df

    def crimes_severity_by_district(self, df=None, img_out=None, csv_out=None, cache=False):
        nypd_df = self.nypd_df

        if df:
            nypd_df = df

        if cache:
            nypd_df = nypd_df.persist()

        # clean dataset from empty BOROUGH
        crimes_df = nypd_df.filter(
            (F.length(F.col(c.BOROUGH)) > 0) & (F.col(c.BOROUGH) != 'false')
        )

        # select only last year
        grouped_crimes_df = crimes_df.select(['yearpd', c.BOROUGH, c.LEVEL_OFFENSE])\
                                     .groupby(['yearpd', c.BOROUGH, c.LEVEL_OFFENSE])\
                                     .count()

        crimes_pddf = grouped_crimes_df.toPandas()

        print(crimes_pddf)

        if img_out:
            fig_dims = (12, 10)
            fig, ax = plt.subplots(figsize=fig_dims)
            sns.catplot(x=c.BOROUGH,  # x variable name
                        y='count',  # y variable name
                        hue='yearpd',  # elements in each group variable name
                        data=crimes_pddf,  # dataframe to plot
                        kind='bar',
                        height=8.27, aspect=11.7 / 8.27)

            plt.xticks(rotation=0)
            plt.savefig(img_out)

        if csv_out:
            self._save_csv(crimes_pddf, csv_out)

        return grouped_crimes_df

    def crimes_day_night(self, df=None, img_out=None, csv_out=None, cache=False):
        nypd_df = self.nypd_df

        if df:
            nypd_df = df

        if cache:
            nypd_df = nypd_df.persist()

        crimes_df = nypd_df.filter(
            F.length(F.col(c.TIME)) > 0
        )

        crimes_df = crimes_df.select([c.DATE, c.TIME, 'yearpd', 'day_night',
                                      c.OFFENSE_DESCRIPTION, c.LEVEL_OFFENSE, c.BOROUGH])

        grouped_crimes_df = crimes_df.groupBy('yearpd', 'day_night', c.LEVEL_OFFENSE).count()

        grouped_crimes_df_day = grouped_crimes_df.filter(F.col('day_night') == 0)
        grouped_crimes_df_night = grouped_crimes_df.filter(F.col('day_night') == 1)

        grouped_crimes_pddf_day = grouped_crimes_df_day.toPandas()
        grouped_crimes_pddf_night = grouped_crimes_df_night.toPandas()

        gr_grouped_crimes_pddf_day = grouped_crimes_pddf_day.groupby(['yearpd']).sum()
        gr_grouped_crimes_pddf_night = grouped_crimes_pddf_night.groupby(['yearpd']).sum()

        print('Day:')
        print(gr_grouped_crimes_pddf_day)

        print('Night:')
        print(gr_grouped_crimes_pddf_night)

        if img_out:
            plt.figure()
            ax = gr_grouped_crimes_pddf_day.unstack().plot()
            gr_grouped_crimes_pddf_night.unstack().plot(ax=ax)
            plt.legend()
            plt.xticks(rotation=0)
            plt.savefig(img_out)

        if csv_out:
            filename_without_ext = csv_out[:-4]
            ext = csv_out[len(csv_out) - 4:]
            self._save_csv(gr_grouped_crimes_pddf_day, filename_without_ext + '_day' + ext)
            self._save_csv(gr_grouped_crimes_pddf_night,  filename_without_ext + '_night' + ext)

        return grouped_crimes_df

    def crimes_race(self, df=None, img_out=None, csv_out=None, cache=False):
        nypd_df = self.nypd_df

        if df:
            nypd_df = df

        if cache:
            nypd_df = nypd_df.persist()

        crime_race_groups = nypd_df.groupBy(c.RACE).count()

        crime_race_counts = crime_race_groups.orderBy('count', ascending=False)

        counts_race_pddf = crime_race_counts.toPandas()
        counts_race_pddf.set_index(c.RACE, inplace=True)
        counts_race_pddf_top8 = counts_race_pddf[:8]

        print(counts_race_pddf_top8)

        if img_out:
            plt.figure()
            counts_race_pddf_top8.plot.pie(y='count')
            plt.savefig(img_out)

        if csv_out:
            self._save_csv(counts_race_pddf_top8, csv_out)

        return crime_race_counts

    def crimes_cross_districts(self, df=None, img_out=None, csv_out=None, cache=False):
        nypd_df = self.nypd_df

        if df:
            nypd_df = df

        if cache:
            nypd_df = nypd_df.persist()

        crimes_df = nypd_df.select(c.BOROUGH, c.OFFENSE_DESCRIPTION).filter(
            (F.length(F.col(c.BOROUGH)) > 0) & (F.length(F.col(c.OFFENSE_DESCRIPTION)) > 0)
        )

        crimes_pddf = self_toPandas(crimes_df, 4)
        df = pd.crosstab(crimes_pddf.BORO_NM, crimes_pddf.OFNS_DESC)

        print(df)

        if img_out:
            plt.figure()
            color = plt.cm.gist_rainbow(np.linspace(0, 1, 10))

            df.div(df.sum(1).astype(float), axis=0).plot.bar(stacked=True, color=color, figsize=(18, 12))
            plt.title('District vs Category of Crime', fontweight=30, fontsize=20)

            plt.xticks(rotation=90)
            plt.savefig(img_out)

        if csv_out:
            self._save_csv(df, csv_out)

        return crimes_df

    def crimes_cross_districts_race(self, df=None, img_out=None, csv_out=None, cache=False):
        nypd_df = self.nypd_df

        if df:
            nypd_df = df

        if cache:
            nypd_df = nypd_df.persist()

        boroughs = ['QUEENS', 'BROOKLYN', 'BRONX', 'STATEN ISLAND', 'MANHATTAN']

        crimes_df = nypd_df.filter(
            (F.length(F.col(c.BOROUGH)) > 0)
            & (F.length(F.col(c.RACE)) > 0)
            & (F.col(c.RACE) != 'UNKNOWN')
            & (F.col(c.BOROUGH).isin(boroughs))
        )

        crimes_df = crimes_df.groupBy(c.BOROUGH, c.RACE).count()

        pddf = self_toPandas(crimes_df)
        print(pddf)

        if csv_out:
            self._save_csv(pddf, csv_out)

        if img_out:
            base_name = img_out[:-4]

            for borough in boroughs:
                plt.figure()
                df = crimes_df.filter(F.col(c.BOROUGH) == borough).sort(F.col(c.RACE))
                _df = df.toPandas()
                _df.set_index(c.RACE, inplace=True)
                _df.plot.pie(y=f'count', figsize=(16, 13), autopct='%1.1f%%').legend(loc='best')

                plt.xticks(rotation=0)
                plt.title('Criminal ethnicity by district: ' + borough)
                plt.savefig(base_name + '_' + str.lower(borough) + '.png')

        return crimes_df

    def crimes_ages(self, df=None, img_out=None, csv_out=None, cache=False):
        nypd_df = self.nypd_df

        if df:
            nypd_df = df

        if cache:
            nypd_df = nypd_df.persist()

        nypd_df = nypd_df.filter(F.length(F.col(c.AGE)) > 0)

        crime_age_groups = nypd_df.groupBy(c.AGE).count()

        crime_age_counts = crime_age_groups.orderBy('count', ascending=False)

        pddf = crime_age_counts.toPandas()
        pddf.set_index(c.AGE, inplace=True)

        if img_out:
            plt.figure()
            pddf.plot.pie(y='count')
            plt.savefig(img_out)

        if csv_out:
            self._save_csv(pddf, csv_out)

        return crime_age_counts

    def cross_district_races(self, df=None, img_out=None, csv_out=None, cache=False):
        nypd_df = self.nypd_df

        if df:
            nypd_df = df

        if cache:
            nypd_df = nypd_df.persist()

        nypd_df = nypd_df.select(c.BOROUGH, c.RACE)

        nypd_df = nypd_df.filter(
            (F.length(F.col(c.BOROUGH)) > 0) & (F.length(F.col(c.RACE)) > 0)
        )

        pddf = self_toPandas(nypd_df)
        df = pd.crosstab(pddf.BORO_NM, pddf.SUSP_RACE)

        if img_out:
            plt.figure()
            color = plt.cm.gist_rainbow(np.linspace(0, 1, 10))

            df.div(df.sum(1).astype(float), axis=0).plot.bar(stacked=True, color=color, figsize=(18, 12))
            plt.title('District vs Category of Crime', fontweight=30, fontsize=20)

            plt.xticks(rotation=90)
            plt.savefig(img_out)

        if csv_out:
            self._save_csv(df, csv_out)

        return nypd_df

    def cross_district_crimes(self, df=None, img_out=None, csv_out=None, cache=False):
        nypd_df = self.nypd_df

        if df:
            nypd_df = df

        if cache:
            nypd_df = nypd_df.persist()

        nypd_df = nypd_df.filter(
            (F.length(F.col(c.BOROUGH)) > 0)
            & (F.length(F.col(c.OFFENSE_DESCRIPTION)) > 0)
        )

        data2 = nypd_df.toPandas()
        df = pd.crosstab(data2.BORO_NM, data2.OFNS_DESC)

        if img_out:
            plt.figure()
            color = plt.cm.gist_rainbow(np.linspace(0, 1, 10))

            df.div(df.sum(1).astype(float), axis=0).plot.bar(stacked=True, color=color, figsize=(18, 12))
            plt.title('District vs Category of Crime', fontweight=30, fontsize=20)

            plt.xticks(rotation=90)
            plt.savefig(img_out)

        if csv_out:
            self._save_csv(df, csv_out)

        return nypd_df

    def cross_age_race(self, df=None, img_out=None, csv_out=None, cache=False):
        nypd_df = self.nypd_df

        if df:
            nypd_df = df

        if cache:
            nypd_df = nypd_df.persist()

        age_groups = ['<18', '18-24', '25-44', '45-64', '65+']

        nypd_df = nypd_df.select(c.AGE, c.RACE)

        nypd_df = nypd_df.filter((F.length(F.col(c.AGE)) > 0) & (F.col(c.AGE) != 'false'))
        nypd_df = nypd_df.where(F.col(c.AGE).isin(age_groups))

        data3 = self_toPandas(nypd_df, 4)
        df = pd.crosstab(data3.SUSP_RACE, data3.SUSP_AGE_GROUP)

        if img_out:
            plt.figure()
            color = plt.cm.gist_rainbow(np.linspace(0, 1, 10))

            df.div(df.sum(1).astype(float), axis=0).plot.bar(stacked=True, color=color, figsize=(18, 12))
            plt.title('age vs race', fontweight=30, fontsize=20)

            plt.xticks(rotation=90)
            plt.savefig(img_out)

        if csv_out:
            self._save_csv(df, csv_out)

        return nypd_df

    def cross_crime_race(self, df=None, img_out=None, csv_out=None, cache=False):
        nypd_df = self.nypd_df

        if df:
            nypd_df = df

        if cache:
            nypd_df = nypd_df.persist()

        nypd_df = nypd_df.select(c.RACE, c.OFFENSE_DESCRIPTION)

        list_df = nypd_df.groupby(c.OFFENSE_DESCRIPTION)\
                         .count()\
                         .orderBy('count', ascending=False)

        top5pd = self_toPandas(list_df)
        top5pd = top5pd[:5]
        top5list = top5pd[c.OFFENSE_DESCRIPTION].values.tolist()

        nypd_df = nypd_df.filter(
            (F.length(F.col(c.RACE)) > 0)
            & (F.col(c.RACE) != '')
            & (F.length(F.col(c.OFFENSE_DESCRIPTION)) > 0)
            & (F.col(c.OFFENSE_DESCRIPTION) != '')
            & (F.col(c.OFFENSE_DESCRIPTION) != 'false')
        )
        nypd_df = nypd_df.where(F.col(c.OFFENSE_DESCRIPTION).isin(top5list))

        data3 = self_toPandas(nypd_df, 4)
        df = pd.crosstab(data3.SUSP_RACE, data3.OFNS_DESC)

        if img_out:
            plt.figure()
            color = plt.cm.gist_rainbow(np.linspace(0, 1, 10))

            df.div(df.sum(1).astype(float), axis=0).plot.bar(stacked=True, color=color, figsize=(18, 12))
            plt.title('CRIMES vs race', fontweight=30, fontsize=20)

            plt.xticks(rotation=90)
            plt.savefig(img_out)

        if csv_out:
            self._save_csv(df, csv_out)

        return nypd_df
