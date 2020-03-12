from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("Predict Adult Salary").getOrCreate()

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

train_df = spark.read.csv('./datasets/train.csv', header=False, schema=schema)
test_df = spark.read.csv('./datasets/test.csv', header=False, schema=schema)

categorical_variables = ['workclass', 'education', 'marital-status',
                         'occupation', 'relationship', 'race', 'sex', 'native-country']
indexers = [StringIndexer(inputCol=column, outputCol=column+"-index").setHandleInvalid("skip")
            for column in categorical_variables]
encoder = OneHotEncoderEstimator(
    inputCols=[indexer.getOutputCol() for indexer in indexers],
    outputCols=["{0}-encoded".format(indexer.getOutputCol())
                for indexer in indexers]
)
assembler = VectorAssembler(
    inputCols=encoder.getOutputCols(),
    outputCol="categorical-features"
)
pipeline = Pipeline(stages=indexers + [encoder, assembler])
train_df = pipeline.fit(train_df).transform(train_df)
test_df = pipeline.fit(test_df).transform(test_df)

# df = train_df.limit(5).toPandas()
# df['scaled-categorical-features'][1]

continuous_variables = ['age', 'fnlwgt', 'education-num',
                        'capital-gain', 'capital-loss', 'hours-per-week']
assembler = VectorAssembler(
    inputCols=['categorical-features', *continuous_variables],
    outputCol='features'
)
train_df = assembler.transform(train_df)
test_df = assembler.transform(test_df)

indexer = StringIndexer(inputCol='salary', outputCol='label')

train_df = indexer.setHandleInvalid("skip").fit(train_df).transform(train_df)
test_df = indexer.setHandleInvalid("skip").fit(test_df).transform(test_df)
# train_df.limit(10).toPandas()['label']

lr = LogisticRegression(featuresCol='features', labelCol='label')
model = lr.fit(train_df)

pred = model.transform(test_df)
pred.limit(10).toPandas()[['label', 'prediction']]
print(pred)
