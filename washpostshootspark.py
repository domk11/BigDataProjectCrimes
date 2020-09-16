from src.spark import create_session, create_df, SparkShoots
from src.database.contracts import wash_contract as c


def main():
    spark = create_session(c.COLLECTION_NAME)
    spark.sparkContext.setLogLevel('ERROR')

    try:
        wpsdf = create_df(spark).cache()
        shoots = SparkShoots(wpsdf)

        monthly_df = shoots.monthly(img_out=True, csv_out=True)
        shoots.yearly(monthly_df, img_out=True, csv_out=True)
        shoots.kills_per_year(img_out=True, csv_out=True)
        shoots.agehist()
        shoots.races(img_out=True, csv_out=True)
        shoots.crimes_per_state(img_out=True, csv_out=True)
        shoots.armed()
        shoots.armed_or_not(img_out=True, csv_out=True)
        shoots.flee(img_out=True, csv_out=True)
        shoots.blacklivesmatter(img_out=True, csv_out=True)
        shoots.allrace(img_out=True, csv_out=True)
    except Exception as e:
        print(e)
        spark.stop()


if __name__ == '__main__':
    main()
