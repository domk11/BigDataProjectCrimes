import time

from src.spark import create_session, create_df, SparkCensus
from src.database.contracts import census_contract as c
from config import PATH


def main():
    spark = create_session(c.FILTERED_COLLECTION)
    spark.sparkContext.setLogLevel('ERROR')

    try:
        census_df = create_df(spark)
        census = SparkCensus(census_df)

        census.race_by_borough(img_out=True, csv_out=True, path=PATH)
    except Exception as e:
        print(e)
    finally:
        spark.stop()


if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
