import pyspark.sql.functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.sql import *
import pandas as pd
from pyspark.sql.types import StructType, StructField, NumericType
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.ml.classification import LogisticRegression


def isSick(x):
    if x in (3, 7):
        return 0
    else:
        return 1


spark = SparkSession.builder.appName("Predict Heart Disease").getOrCreate()

cols = ('age',
        'sex',
        'chest pain',
        'resting blood pressure',
        'serum cholesterol',
        'fasting blood sugar',
        'resting electrocardiographic results',
        'maximum heart rate achieved',
        'exercise induced angina',
        'ST depression induced by exercise relative to rest',
        'the slope of the peak exercise ST segment',
        'number of major vessels ',
        'thal',
        'last')


data = pd.read_csv('./datasets/heart.csv', delimiter=' ', names=cols)

data = data.iloc[:, 0:13]

data['isSick'] = data['thal'].apply(isSick)

df = spark.createDataFrame(data)

print("HELLOO", df)
print("\n")


features = ('age',
            'sex',
            'chest pain',
            'resting blood pressure',
            'serum cholesterol',
            'fasting blood sugar',
            'resting electrocardiographic results',
            'maximum heart rate achieved',
            'exercise induced angina',
            'ST depression induced by exercise relative to rest',
            'the slope of the peak exercise ST segment',
            'number of major vessels ')

assembler = VectorAssembler(inputCols=features, outputCol="features")

raw_data = assembler.transform(df)
raw_data.select("features").show(truncate=False)

standardscaler = StandardScaler().setInputCol(
    "features").setOutputCol("Scaled_features")
raw_data = standardscaler.fit(raw_data).transform(raw_data)
raw_data.select("features", "Scaled_features").show(5)


training, test = raw_data.randomSplit([0.5, 0.5], seed=12345)


lr = LogisticRegression(
    labelCol="isSick", featuresCol="Scaled_features", maxIter=10)
model = lr.fit(training)
predict_train = model.transform(training)
predict_test = model.transform(test)
predict_test.select("isSick", "prediction").show(10)

print("Multinomial coefficients: " + str(model.coefficientMatrix))
print("Multinomial intercepts: " + str(model.interceptVector))

check = predict_test.withColumn('correct', F.when(
    F.col('isSick') == F.col('prediction'), 1).otherwise(0))
check.groupby("correct").count().show()
