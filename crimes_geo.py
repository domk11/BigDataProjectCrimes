import geopandas as geopd
from src.spark import create_session, create_df
import pyspark.sql.functions as F
from pyspark.sql.types import *
from src.database.contracts import nypd_contract as c
import matplotlib.pyplot as plt
from src.spark.schema import COLUMNS
from utils import *
import matplotlib.pyplot as plt
from shapely.geometry import Point, Polygon


def plot_city_crimes(nypd_df):

    boros = geopd.read_file(geopd.datasets.get_path("nybb"))
    boro_locations = geopd.tools.geocode(boros.BoroName)

    # get rid of missing values
    crimes_df = nypd_df.filter(
        (F.length(F.col(c.LATITUDE)) > 0) & (F.length(F.col(c.LONGITUDE)) > 0) &
        (F.length(F.col(c.LEVEL_OFFENSE)) > 0) & (F.length(F.col(c.BOROUGH)) > 0)
    ).select(['yearpd', c.LATITUDE, c.LONGITUDE, c.LEVEL_OFFENSE, c.BOROUGH])

    crimes_df = crimes_df.groupby([c.BOROUGH]).count()

    geometry_crimes_pddf = crimes_df.toPandas()
    print(geometry_crimes_pddf.head())
    #geometry = [Point(x,y) for x,y in zip(geometry_crimes_pddf[c.LATITUDE], geometry_crimes_pddf[c.LONGITUDE])]

    # convert Latitude, Longitude to geopandas Point
    #geometry_crimes_pddf['geometry'] = geometry
    #print(geometry_crimes_pddf.head())


    # fig, ax = plt.subplots()
    # boros.to_crs("EPSG:4326").plot(ax=ax, color="white", edgecolor="black")
    # # boro_locations.plot(ax=ax, color="red")
    # plt.savefig("ciao.png")


def main():
    spark = create_session()
    spark.sparkContext.setLogLevel('ERROR')
    sc = spark.sparkContext

    try:

        # nypd_crimes_rdd = create_rdd(spark, COLUMNS)
        nypd_df = create_df(spark, COLUMNS)
        nypd_df = fix_date_nypd(nypd_df, ['2009', '2019']).cache()
        #plot_city_crimes(nypd_df)



    except Exception as e:
        print(e)
    finally:
        sc.stop()
        spark.stop()



if __name__ == '__main__':
    main()