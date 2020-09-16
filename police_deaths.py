from src.spark import create_session, create_df
from src.spark.SparkPDDE import SparkPDDE
from src.database.contracts import police_contract as c
from config import PATH


def main():

    spark = create_session(c.FILTERED_COLLECTION)
    spark.sparkContext.setLogLevel('ERROR')

    try:
        nypd_df = create_df(spark)

        pd = SparkPDDE(nypd_df)
        pd.deaths_trend(img_out=f'{PATH}/police_deaths.png',
                        csv_out=f'{PATH}/police_deaths.csv')
        pd.deaths_cause_topN(img_out=f'{PATH}/police_top_deaths.png',
                             csv_out=f'{PATH}/police_top_deaths.csv')
        pd.deaths_states_topN(n=10, img_out=f'{PATH}/police_deaths_state.png',
                              csv_out=f'{PATH}/police_deaths_state.csv')

        print('Done')
    except Exception as e:
        print(e)
    finally:
        spark.sparkContext.stop()


if __name__ == '__main__':
    import time

    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
