from src.spark import create_session, create_df
from src.spark.SparkPDDE import SparkPDDE
from src.database.contracts import police_contract as c


def main():

    spark = create_session(c.FILTERED_COLLECTION)
    spark.sparkContext.setLogLevel('ERROR')

    try:

        output_base = '/home/marco/output/'
        nypd_df = create_df(spark)

        pd = SparkPDDE(nypd_df)
        pd.deaths_trend(img_out=output_base + 'police_deaths.png')
        pd.deaths_cause_topN(img_out=output_base + 'police_top_deaths.png')
        pd.deaths_states_topN(n=10, img_out=output_base + 'police_deaths_state.png')

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
