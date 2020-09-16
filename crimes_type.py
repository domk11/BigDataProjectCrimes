import time

from src.spark import create_session, create_df
from src.spark.SparkNYPD import SparkNYPD
from src.database.contracts import nypd_contract as c
from config import PATH


def main():
    spark = create_session(c.FILTERED_COLLECTION)
    spark.sparkContext.setLogLevel('ERROR')

    try:
        nypd_df = create_df(spark)
        ny = SparkNYPD(nypd_df)

        ny.crimes_trend(img_out=f'{PATH}/crimes_trend.png', csv_out=f'{PATH}/crimes_trend.csv')
        ny.crimes_top(img_out=f'{PATH}/crimes_top.png', csv_out=f'{PATH}/crimes_top.csv')
        ny.crimes_severity(img_out=f'{PATH}/crimes_severity.png', csv_out=f'{PATH}/crimes_severity.csv')
        ny.crimes_severity_by_district(img_out=f'{PATH}/crimes_severity_by_district.png', csv_out=f'{PATH}/crimes_severity_by_district.csv')
        ny.crimes_day_night(img_out=f'{PATH}/crimes_daynight.png', csv_out=f'{PATH}/crimes_daynight.csv')
        ny.crimes_race(img_out=f'{PATH}/crimes_race.png', csv_out=f'{PATH}/crimes_race.csv')
        ny.crimes_cross_districts(img_out=f'{PATH}/crimes_cross_districts.png', csv_out=f'{PATH}/crimes_cross_districts.csv')
        ny.crimes_cross_districts_race(img_out=f'{PATH}/crimes_districts_race.png', csv_out=f'{PATH}/crimes_districts_race.csv')
        ny.cross_age_race(img_out=f'{PATH}/crimes_cross_age_race.png', csv_out=f'{PATH}/crimes_cross_age_race.csv')
        ny.cross_crime_race(img_out=f'{PATH}/crimes_cross_crime_race.png', csv_out=f'{PATH}/crimes_cross_crime_race.csv')

        print('Done')
    except Exception as e:
        print(e)
    finally:
        spark.stop()


if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
