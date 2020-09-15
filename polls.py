import time

from src.spark import create_session, create_df, SparkPolitics
from src.database.contracts import politics_contract as c


def main():
    spark = create_session(c.COLLECTION_NAME)
    spark.sparkContext.setLogLevel('ERROR')

    try:
        politics_df = create_df(spark)
        politics = SparkPolitics(politics_df)

        politics.polls_map(img_out=True)
    except Exception as e:
        print(e)
    finally:
        spark.stop()


if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
