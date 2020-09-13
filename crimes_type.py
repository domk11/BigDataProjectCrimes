from src.spark import create_session, create_df
from src.spark.SparkNYPD import SparkNYPD
from src.database.contracts import nypd_contract as c


def main():
    spark = create_session(c.FILTERED_COLLECTION)
    spark.sparkContext.setLogLevel('ERROR')

    try:

        output_base = '/home/marco/output/'
        nypd_df = create_df(spark)

        ny = SparkNYPD(nypd_df)

        # ny.crimes_trend(img_out=output_base + 'crimes_trend.png',
        #                 csv_out=output_base + 'crimes_trend.csv')
        #
        # ny.crimes_top(img_out=output_base + 'crimes_top.png',
        #               csv_out=output_base + 'crimes_top.csv')
        #
        # ny.crimes_severity(img_out=output_base + 'crimes_severity.png',
        #                    csv_out=output_base + 'crimes_severity.csv')
        #
        # # TODO: fix error in plot
        # # ny.crimes_severity_by_district(img_out=output_base + 'crimes_severity_by_district.png',
        # #                                csv_out=output_base + 'crimes_severity_by_district.csv')
        #
        # ny.crimes_day_night(img_out=output_base + 'crimes_daynight.png',
        #                     csv_out=output_base + 'crimes_daynight.csv')
        #
        # ny.crimes_race(img_out=output_base + 'crimes_race.png',
        #                csv_out=output_base + 'crimes_race.csv')
        #
        # ny.crimes_cross_districts(img_out=output_base + 'crimes_cross_districts.png',
        #                           csv_out=output_base + 'crimes_cross_districts.csv')
        #
        # ny.crimes_cross_districts_race(img_out=output_base + 'crimes_districts_race.png',
        #                                csv_out=output_base + 'crimes_districts_race.csv')

        print("Done")

    except Exception as e:
        print(e)
    finally:
        spark.sparkContext.stop()


if __name__ == '__main__':
    import time

    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
