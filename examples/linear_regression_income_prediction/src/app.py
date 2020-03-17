from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pandas as pd


## Supporting Functions
def getCsv():
    column_names = [
        'age',
        'workclass',
        'fnlwgt',
        'education',
        'education-num',
        'marital-status',
        'occupation',
        'relationship',
        'race',
        'sex',
        'capital-gain',
        'capital-loss',
        'hours-per-week',
        'native-country',
        'salary'
    ]

    train_df = pd.read_csv(
        './linear_regression_income_prediction/src/inputData/adult.data',
        names=column_names)
    test_df = pd.read_csv(
        './linear_regression_income_prediction/src/inputData/adult.test',
        names=column_names)
    train_df = train_df.apply(lambda x: x.astype(str).str.strip() if x.dtype == 'object' else x)
    train_df_cp = train_df.copy()
    train_df_cp = train_df_cp.loc[train_df_cp['native-country'] != 'Holand-Netherlands']
    train_df_cp.to_csv('train.csv', index=False, header=False)
    test_df = test_df.apply(lambda x: x.astype(str).str.strip() if x.dtype == 'object' else x)
    test_df.to_csv('test.csv', index=False, header=False)


def main(spark):
    getCsv()

    schema = StructType([
        StructField("age", IntegerType(), True),
        StructField("workclass", StringType(), True),
        StructField("fnlwgt", IntegerType(), True),
        StructField("education", StringType(), True),
        StructField("education-num", IntegerType(), True),
        StructField("marital-status", StringType(), True),
        StructField("occupation", StringType(), True),
        StructField("relationship", StringType(), True),
        StructField("race", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("capital-gain", IntegerType(), True),
        StructField("capital-loss", IntegerType(), True),
        StructField("hours-per-week", IntegerType(), True),
        StructField("native-country", StringType(), True),
        StructField("salary", StringType(), True)
    ])

    train_df = spark.read.csv('train.csv', header=False, schema=schema)
    test_df = spark.read.csv('test.csv', header=False, schema=schema)

    print(train_df.limit(5).toPandas())

    categorical_variables = ['workclass', 'education', 'marital-status', 'occupation', 'relationship', 'race', 'sex',
                             'native-country']
    indexers = [StringIndexer(inputCol=column, outputCol=column + "-index") for column in categorical_variables]

    encoder = OneHotEncoderEstimator(
        inputCols=[indexer.getOutputCol() for indexer in indexers],
        outputCols=["{0}-encoded".format(indexer.getOutputCol()) for indexer in indexers]
    )

    assembler = VectorAssembler(
        inputCols=encoder.getOutputCols(),
        outputCol="categorical-features"
    )
    pipeline = Pipeline(stages=indexers + [encoder, assembler])
    train_df = pipeline.fit(train_df).transform(train_df)
    test_df = pipeline.fit(test_df).transform(test_df)

    train_df.printSchema()

    df = train_df.limit(5).toPandas()
    print(df['categorical-features'][1])

    continuous_variables = ['age', 'fnlwgt', 'education-num', 'capital-gain', 'capital-loss', 'hours-per-week']
    assembler = VectorAssembler(
        inputCols=['categorical-features', *continuous_variables],
        outputCol='features'
    )
    train_df = assembler.transform(train_df)
    test_df = assembler.transform(test_df)
    print(train_df.limit(5).toPandas()['features'][0])

    indexer = StringIndexer(inputCol='salary', outputCol='label', handleInvalid="skip")
    train_df = indexer.fit(train_df).transform(train_df)
    test_df = indexer.fit(test_df).transform(test_df)

    lr = LogisticRegression(featuresCol='features', labelCol='label')
    model = lr.fit(train_df)

    pred = model.transform(test_df)

    # print(pred.limit(10).toPandas()[['label', 'prediction']])


if __name__ == "__main__":
    # Configure Spark Application
    spark = SparkSession.builder.appName("linear_regression_income_prediction").getOrCreate()

    # Execute Main functionality
    main(spark)
