from pyspark.sql import SQLContext
from pyspark.sql import DataFrameNaFunctions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import Binarizer
from pyspark.ml.feature import OneHotEncoder, VectorAssembler, StringIndexer, VectorIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.sql.functions import avg
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics, BinaryClassificationMetrics as metric
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from imblearn.over_sampling import SMOTE
from imblearn.combine import SMOTEENN
from sklearn.model_selection import train_test_split
from collections import Counter

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

# spark = SparkSession.builder.appName("Predict Stroke").getOrCreate()

schema = StructType([
    StructField("gender", StringType(), True),
    StructField("age", DoubleType(), True),
    StructField("diabetes", IntegerType(), True),
    StructField("hypertension", StringType(), True),
    StructField("stroke", IntegerType(), True),
    StructField("heart disease", IntegerType(), True),
    StructField("smoking history", StringType(), True),
    StructField("BMI", DoubleType(), True),
])


df = spark.read.format('com.databricks.spark.csv').options(header=True, inferschema=True).load('./datasets/stroke.csv')

# df = spark.read.csv('./datasets/stroke.csv', header=True, schema=schema)
# print(df.columns)

featureColumns = ['gender','age','diabetes','hypertension','heart disease','smoking history','BMI']

print(df.printSchema())

df = df.filter(df.age >2)
print(df.count())

imputeDF = df
imputeDF_Pandas = imputeDF.toPandas()

df_2_9 = imputeDF_Pandas[(imputeDF_Pandas['age'] >=2 ) & (imputeDF_Pandas['age'] <= 9)]
values = {'smoking history': 0, 'BMI':17.125}
df_2_9 = df_2_9.fillna(value = values)
df_10_13 = imputeDF_Pandas[(imputeDF_Pandas['age'] >=10 ) & (imputeDF_Pandas['age'] <= 13)]
values = {'smoking history': 0, 'BMI':19.5}
df_10_13 = df_10_13.fillna(value = values)
df_14_17 = imputeDF_Pandas[(imputeDF_Pandas['age'] >=14 ) & (imputeDF_Pandas['age'] <= 17)]
values = {'smoking history': 0, 'BMI':23.05}
df_14_17 = df_14_17.fillna(value = values)
df_18_24 = imputeDF_Pandas[(imputeDF_Pandas['age'] >=18 ) & (imputeDF_Pandas['age'] <= 24)]
values = {'smoking history': 0, 'BMI':27.1}
df_18_24 = df_18_24.fillna(value = values)
df_25_29 = imputeDF_Pandas[(imputeDF_Pandas['age'] >=25 ) & (imputeDF_Pandas['age'] <= 29)]
values = {'smoking history': 0, 'BMI':27.9}
df_25_29 = df_25_29.fillna(value = values)
df_30_34 = imputeDF_Pandas[(imputeDF_Pandas['age'] >=30 ) & (imputeDF_Pandas['age'] <= 34)]
values = {'smoking history': 0.25, 'BMI':29.6}
df_30_34 = df_30_34.fillna(value = values)
df_35_44 = imputeDF_Pandas[(imputeDF_Pandas['age'] >=35 ) & (imputeDF_Pandas['age'] <= 44)]
values = {'smoking history': 0.25, 'BMI':30.15}
df_35_44 = df_35_44.fillna(value = values)
df_45_49 = imputeDF_Pandas[(imputeDF_Pandas['age'] >=45 ) & (imputeDF_Pandas['age'] <= 49)]
values = {'smoking history': 0, 'BMI':29.7}
df_45_49 = df_45_49.fillna(value = values)
df_50_59 = imputeDF_Pandas[(imputeDF_Pandas['age'] >=50 ) & (imputeDF_Pandas['age'] <= 59)]
values = {'smoking history': 0, 'BMI':29.95}
df_50_59 = df_50_59.fillna(value = values)
df_60_74 = imputeDF_Pandas[(imputeDF_Pandas['age'] >=60 ) & (imputeDF_Pandas['age'] <= 74)]
values = {'smoking history': 0, 'BMI':30.1}
df_60_74 = df_60_74.fillna(value = values)
df_75_plus = imputeDF_Pandas[(imputeDF_Pandas['age'] >75 )]
values = {'smoking history': 0, 'BMI':28.1}
df_75_plus = df_75_plus.fillna(value = values)


all_frames = [df_2_9, df_10_13, df_14_17, df_18_24, df_25_29, df_30_34, df_35_44, df_45_49, df_50_59, df_60_74, df_75_plus]
df_combined = pd.concat(all_frames)

print(df_combined, df_combined.columns, df.dtypes)

df_combined_converted = spark.createDataFrame(df_combined, schema=schema)
imputeDF = df_combined_converted

print("CONVERTED",imputeDF)

X = imputeDF.toPandas().filter(items=['gender', 'age', 'diabetes','hypertension','heart disease','smoking history','BMI'])
Y = imputeDF.toPandas().filter(items=['stroke'])
X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.1, random_state=0)

# SMOTE technique for resampling
sm = SMOTE(random_state=12, ratio='auto', kind='regular')
x_train_res, y_train_res = sm.fit_sample(X_train, Y_train)
print('Resampled dataset shape {}'.format(Counter(y_train_res)))

dataframe_1 = pd.DataFrame(x_train_res,columns=['gender', 'age', 'diabetes', 'hypertension', 'heart disease', 'smoking history', 'BMI'])
dataframe_2 = pd.DataFrame(y_train_res, columns = ['stroke'])
# frames = [dataframe_1, dataframe_2]
result = dataframe_1.combine_first(dataframe_2)

imputeDF_1 = spark.createDataFrame(result)

binarizer = Binarizer(threshold=0.0, inputCol="stroke", outputCol="label")
binarizedDF = binarizer.transform(imputeDF_1)
binarizedDF = binarizedDF.drop('stroke')

assembler = VectorAssembler(inputCols = featureColumns, outputCol = "features")
assembled = assembler.transform(imputeDF)
print(assembled)

(trainingData, testData) = assembled.randomSplit([0.7, 0.3], seed=0)
print("Distribution of Ones and Zeros in trainingData is: ", trainingData.groupBy("label").count().take(3))

dt = DecisionTreeClassifier(labelCol="label", featuresCol="features", maxDepth=25, minInstancesPerNode=30, impurity="gini")
pipeline = Pipeline(stages=[dt])
model = pipeline.fit(trainingData)

predictions = model.transform(testData)
print(predictions)




# results = predictions.select(['probability', 'label'])
 
# ## prepare score-label set
# results_collect = results.collect()
# results_list = [(float(i[0][0]), 1.0-float(i[1])) for i in results_collect]
# scoreAndLabels = sc.parallelize(results_list)
 
# metrics = metric(scoreAndLabels)
# print("Test Data Aread under ROC score is : ", metrics.areaUnderROC)