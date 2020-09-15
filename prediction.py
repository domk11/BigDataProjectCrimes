# Just for test with PySpark MLib not really useful for the abalysis
#
#


from src.spark import create_session, create_df
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.feature import OneHotEncoderEstimator, OneHotEncoder
from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
import pyspark.sql.functions as F


def _parse_race(race):
    if race == 'W':
        return 1
    else:
        return 0


udf_parse_race = F.udf(_parse_race)


class Shootings:
    shootings_df = None

    def __init__(self, df):
        self.shootings_df = df
        self._preprocess()

    def _preprocess(self):

        input_cols = ['armed', 'city', 'manner_of_death', 'flee', 'gender', 'state',
                      'threat_level', 'body_camera', 'signs_of_mental_illness']

        # self.shootings_df = self.shootings_df.select([c for c in self.shootings_df.columns if c in input_cols])
        # self.shootings_df.show(n=10)

        self.shootings_df = self.shootings_df.filter(
            (F.length(F.col('armed')) > 0) & (F.length(F.col('city')) > 0) & \
            (F.length(F.col('manner_of_death')) > 0) & (F.length(F.col('race')) > 0) & \
            (F.length(F.col('flee')) > 0) & (F.length(F.col('gender')) > 0) & \
            (F.length(F.col('state')) > 0) & (F.length(F.col('threat_level')) > 0) & \
            (F.length(F.col('body_camera')) > 0) & (F.length(F.col('signs_of_mental_illness')) > 0)
            )

        indexers = [
            StringIndexer(inputCol=c, outputCol="{0}_indexed".format(c))
            for c in input_cols
        ]

        # The encode of indexed vlaues multiple columns
        encoders = [OneHotEncoder(dropLast=False, inputCol=indexer.getOutputCol(),
                                  outputCol="{0}_enc".format(indexer.getOutputCol()))
                    for indexer in indexers
                    ]

        # Vectorizing encoded values
        assembler = VectorAssembler(inputCols=[encoder.getOutputCol() for encoder in encoders], outputCol="features")



        pipeline = Pipeline(stages=indexers + encoders + [assembler])
        model = pipeline.fit(self.shootings_df)
        self.shootings_df = model.transform(self.shootings_df)

        self.shootings_df = self.shootings_df.withColumn('label', udf_parse_race('race').cast('int'))
        self.shootings_df = self.shootings_df.select('features', 'race', 'label')
        self.shootings_df.persist().count()

        return self.shootings_df

    def show(self, n=10):
        self.shootings_df.show(n, truncate=False)

    def get_df(self):
        return self.shootings_df

    def get_train(self, df=None):
        shootings_df = self.shootings_df
        if df:
            shootings_df = df
        return shootings_df.select('features')

    def get_test(self, df=None):
        shootings_df = self.shootings_df
        if df:
            shootings_df = df
        return shootings_df.select('label')


def main():

    spark = create_session('wash_post_shootings')
    spark.sparkContext.setLogLevel('ERROR')

    try:

        output_base = '/home/marco/output/'
        df = create_df(spark)

        sh = Shootings(df)
        sh.show()

        df = sh.get_df()

        (train, test) = df.randomSplit([0.8, 0.2])

        # instantiate the base classifier.
        lr = LogisticRegression(maxIter=10, tol=1E-6, fitIntercept=True, featuresCol='features', labelCol='label')

        # instantiate the One Vs Rest Classifier.
        ovr = OneVsRest(classifier=lr)

        # train the multiclass model.
        ovrModel = ovr.fit(train)

        # score the model on test data.
        predictions = ovrModel.transform(test)

        # obtain evaluator.
        evaluator = MulticlassClassificationEvaluator(metricName="accuracy")

        # compute the classification error on test data.
        accuracy = evaluator.evaluate(predictions)
        print("Test Error = %g" % (1.0 - accuracy))
        print("Accuracy = %.2f" % (accuracy * 100))


    except Exception as e:
        print(e)
    finally:
        spark.sparkContext.stop()


if __name__ == '__main__':
    main()