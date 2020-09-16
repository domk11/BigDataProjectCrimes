from pyspark import StorageLevel
import pyspark.sql.functions as F
import matplotlib.pyplot as plt

from src.database.contracts import census_contract as c
from utils import udf_convert_race_census, udf_convert_county


class SparkCensus:

    def __init__(self, census_df):
        self.census_df = census_df
        self._preprocess()

    def _preprocess(self):
        self.census_df = self.census_df.withColumn(c.RACE, udf_convert_race_census(c.RACE))
        self.census_df.persist(StorageLevel.MEMORY_AND_DISK).count()

    def _save_csv(self, df, csv_out):
        df.toPandas().to_csv(csv_out)

    def show_df(self, df, limit=20):
        df.show(limit)

    def race_by_borough(self, img_out=False, csv_out=None, cache=False):
        census_df = self.census_df

        if cache:
            census_df = census_df.persist()

        race_county_df = census_df.select([c.COUNTY, c.RACE, c.POP]) \
                                  .groupby([c.COUNTY, c.RACE]) \
                                  .sum()

        county = ['New York County', 'Kings County', 'Queens County', 'Bronx County', 'Richmond County']
        race_county_df = race_county_df.filter(F.col(c.COUNTY).isin(county))

        race_borough_df = race_county_df.withColumn(c.COUNTY, udf_convert_county(c.COUNTY))

        if csv_out:
            self._save_csv(race_county_df, csv_out)

        if img_out:
            boroughs = ['MANHATTAN', 'BROOKLYN', 'QUEENS', 'BRONX', 'STATEN ISLAND']
            for borough in boroughs:
                df = race_borough_df.filter(F.col(c.COUNTY) == borough) \
                                    .sort(F.col(c.RACE)) \
                                    .drop(c.COUNTY)
                pd = df.toPandas()
                pd.set_index(c.RACE, inplace=True)
                pd.plot.pie(y=f'sum({c.POP})', figsize=(16, 13), autopct='%1.1f%%').legend(loc='best')
                plt.ylabel('population distribution')
                plt.title(borough)
                plt.savefig(f'{borough.lower().replace(" ", "_")}.png')
