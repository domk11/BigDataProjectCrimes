from src.spark import create_session, create_df, SparkShoots
from src.database.contracts import wash_contract as c
from config import PATH


def main():
    spark = create_session(c.COLLECTION_NAME)
    spark.sparkContext.setLogLevel('ERROR')

    try:
        wpsdf = create_df(spark).cache()
        shoots = SparkShoots(wpsdf)

        monthly_df = shoots.monthly(img_out=True, csv_out=True, path=PATH)
        shoots.yearly(monthly_df, img_out=True, csv_out=True, path=PATH)
        shoots.kills_per_year(img_out=True, csv_out=True, path=PATH)
        shoots.agehist(img_out=True, path=PATH)
        shoots.races(img_out=True, csv_out=True, path=PATH)
        shoots.crimes_per_state(img_out=True, csv_out=True, path=PATH)
        shoots.armed(img_out=True, path=PATH)
        shoots.armed_or_not(img_out=True, csv_out=True, path=PATH)
        shoots.flee(img_out=True, csv_out=True, path=PATH)
        shoots.blacklivesmatter(img_out=True, csv_out=True, path=PATH)
        shoots.allrace(img_out=True, csv_out=True, path=PATH)
    except Exception as e:
        print(e)
        spark.stop()


if __name__ == '__main__':
    main()
