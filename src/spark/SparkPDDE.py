import pyspark.sql.functions as F
from pyspark import StorageLevel
from pyspark.sql.types import *

import matplotlib.pyplot as plt
import pandas as pd
from sklearn.linear_model import LinearRegression

from src.database.contracts import police_contract as c

plt.rcParams['figure.figsize'] = (12, 8)


class SparkPDDE:

    pdde_df = None

    def __init__(self, pdde_df):
        self.pdde_df = pdde_df
        self._preprocess()

    def _preprocess(self):
        self.pdde_df = self.pdde_df.withColumn(c.CANINE, F.when(F.col(c.CANINE) == 'TRUE', 1).otherwise(0).cast('int'))\
                                   .withColumn(c.STATE, F.trim(F.col(c.STATE)))

        self.pdde_df = self.pdde_df.filter(
            (F.col(c.YEAR) > 2008) & (F.col(c.YEAR) < 2020)
        )

        # trigger the cache
        self.pdde_df.persist(StorageLevel.MEMORY_AND_DISK).count()

    def deaths_cause_topN(self, n=10, df=None, img_out=None, cache=False):

        pdde_df = self.pdde_df

        if df:
            pdde_df = df

        if cache:
            pdde_df = pdde_df.persist()

        # data cleaning:
        # filter rows without a CoD
        df = pdde_df.filter(F.length(F.col(c.CAUSE)) > 0)

        # deaths types
        deaths_types = df.groupBy(c.CAUSE).count()
        deaths_types_counts = deaths_types.orderBy('count', ascending=False)

        # select the top N most frequent deaths causes and plot the distribution
        counts_deaths_ppdf = deaths_types_counts.toPandas()
        counts_deaths_pddf_top_N = counts_deaths_ppdf[:n]

        print(counts_deaths_pddf_top_N)

        if img_out:
            counts_deaths_pddf_top_N.plot.barh(x=c.CAUSE, y='count')
            plt.xlabel('Most frequent causes of deaths in policemen')
            plt.ylabel('Count')
            plt.savefig(img_out)

        return counts_deaths_pddf_top_N

    def deaths_states_topN(self, n=5, df=None, img_out=None, cache=False):

        pdde_df = self.pdde_df

        if df:
            pdde_df = df

        if cache:
            pdde_df = pdde_df.persist()

        # data cleaning:
        # filter rows without a state
        df = pdde_df.filter(F.length(F.col(c.STATE)) > 0)

        # states
        deaths_states = df.groupBy(c.STATE).count()
        deaths_states_counts = deaths_states.orderBy('count', ascending=False)

        # select the top N most frequent deaths causes and plot the distribution
        counts_states_ppdf = deaths_states_counts.toPandas()
        counts_deaths_pddf_top_N = counts_states_ppdf[:n]

        print(counts_deaths_pddf_top_N)

        if img_out:
            counts_deaths_pddf_top_N.plot.barh(x=c.STATE, y='count')
            plt.xlabel('States with more deaths in Police')
            plt.ylabel('Count')
            plt.savefig(img_out)

        return counts_deaths_pddf_top_N


    def deaths_trend(self, df=None, img_out=None, cache=False):

        pdde_df = self.pdde_df

        if df:
            pdde_df = df

        if cache:
            pdde_df = pdde_df.persist()

        deaths_df = pdde_df.groupby(c.YEAR).count().orderBy(c.YEAR)

        pddf = deaths_df.toPandas()
        X = pddf[c.YEAR].values.reshape(-1, 1)
        Y = pddf['count'].values.reshape(-1, 1)

        linear_regressor = LinearRegression()  # create object for the class
        linear_regressor.fit(X, Y)  # perform linear regression
        Y_pred = linear_regressor.predict(X)  # make predictions

        print(pddf)

        if img_out:
            fig, ax = plt.subplots()

            ax.plot(X, Y, label='Police Deaths')
            ax.plot(X, Y_pred, '--', label='Trend')
            ax.set(xlabel=f'Year - 2009-2016',
                   ylabel='Total records',
                   title='Year-on-year police deaths records')
            ax.grid(b=True, which='both', axis='y')
            ax.legend()
            plt.savefig(img_out)

        return deaths_df

