from pyspark import StorageLevel
import pyspark.sql.functions as F
import matplotlib.pyplot as plt

from src.database.contracts import census_contract as c


def _convert_race(race):
    race = race.split()
    r = {
        'American': 'AMERICAN INDIAN/ALASKAN NATIVE',
        'Asian': 'ASIAN / PACIFIC ISLANDER',
        'Native': 'ASIAN / PACIFIC ISLANDER'
    }.get(race[0])

    if race[0] == 'Black':
        r = 'BLACK HISPANIC' if race[-1] == 'Hispanic' else 'BLACK'

    if race[0] == 'White':
        r = 'WHITE HISPANIC' if race[-1] == 'Hispanic' else 'WHITE'

    return r


def _convert_county(county):
    return {
        'New York County': 'MANHATTAN',
        'Kings County': 'BROOKLYN',
        'Queens County': 'QUEENS',
        'Bronx County': 'BRONX',
        'Richmond County': 'STATEN ISLAND'
    }.get(county)

class SparkCensus:

    def __init__(self, census_df):
        self.census_df = census_df
        self._preprocess()

    def _preprocess(self):
        convert = F.udf(_convert_race)
        self.census_df = self.census_df.withColumn(c.RACE, convert(c.RACE))
        self.census_df.persist(StorageLevel.MEMORY_AND_DISK).count()

    def _save_csv(self, df, csv_out):
        df.toPandas().to_csv(csv_out)

    def show_df(self, df, limit=20):
        df.show(limit)

    def race_by_borough(self, img_out=None, csv_out=None, cache=False):
        census_df = self.census_df

        if cache:
            census_df = census_df.persist()

        race_county_df = census_df.select([c.COUNTY, c.RACE, c.POP]) \
                                  .groupby([c.COUNTY, c.RACE]) \
                                  .sum()

        county = ['New York County', 'Kings County', 'Queens County', 'Bronx County', 'Richmond County']
        race_county_df = race_county_df.filter(F.col(c.COUNTY).isin(county))

        convert = F.udf(_convert_county)
        race_borough_df = race_county_df.withColumn(c.COUNTY, convert(c.COUNTY))

        if csv_out:
            self._save_csv(race_borough_df, csv_out)

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
                plt.savefig(img_out + f'{borough.lower().replace(" ", "_")}.png')
