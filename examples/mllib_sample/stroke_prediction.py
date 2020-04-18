from pyspark.sql import SparkSession
import pyspark.sql as sparksql
from pyspark.sql.functions import mean
from pyspark.ml.feature import (VectorAssembler,OneHotEncoder,
                                StringIndexer)
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.appName('stroke').getOrCreate()
train = spark.read.csv('./datasets/stroke_prediction.csv', inferSchema=True, header=True)

print(train.head(5))

# fill in missing values
train_f = train.na.fill('No Info', subset=['smoking_status'])
# fill in miss values with mean

mean = train_f.select(mean(train_f['bmi'])).collect()
mean_bmi = mean[0][0]
train_f = train_f.na.fill(mean_bmi,['bmi'])

gender_indexer = StringIndexer(inputCol='gender', outputCol='genderIndex')
gender_encoder = OneHotEncoder(inputCol='genderIndex', outputCol='genderVec')

ever_married_indexer = StringIndexer(inputCol='ever_married', outputCol='ever_marriedIndex')
ever_married_encoder = OneHotEncoder(inputCol='ever_marriedIndex', outputCol='ever_marriedVec')

work_type_indexer = StringIndexer(inputCol='work_type', outputCol='work_typeIndex')
work_type_encoder = OneHotEncoder(inputCol='work_typeIndex', outputCol='work_typeVec')

smoking_status_indexer = StringIndexer(inputCol='smoking_status', outputCol='smoking_statusIndex')
smoking_status_encoder = OneHotEncoder(inputCol='smoking_statusIndex', outputCol='smoking_statusVec')

Residence_type_indexer = StringIndexer(inputCol='Residence_type', outputCol='Residence_typeIndex')
Residence_type_encoder = OneHotEncoder(inputCol='Residence_typeIndex', outputCol='Residence_typeVec')

assembler = VectorAssembler(inputCols=['genderVec',
 'age',
 'hypertension',
 'heart_disease',
 'ever_marriedVec',
 'work_typeVec',
 'Residence_typeVec',
 'avg_glucose_level',
 'bmi',
 'smoking_statusVec'],outputCol='features')

dtc = DecisionTreeClassifier(labelCol='stroke',featuresCol='features')

pipeline = Pipeline(stages=[gender_indexer, ever_married_indexer, work_type_indexer, Residence_type_indexer,
                           smoking_status_indexer, gender_encoder, ever_married_encoder, work_type_encoder,
                           Residence_type_encoder, smoking_status_encoder, assembler, dtc])

train_data,test_data = train_f.randomSplit([0.7,0.3])

model = pipeline.fit(train_data)

dtc_predictions = model.transform(test_data)

# Select (prediction, true label) and compute test error
acc_evaluator = MulticlassClassificationEvaluator(labelCol="stroke", predictionCol="prediction", metricName="accuracy")
dtc_acc = acc_evaluator.evaluate(dtc_predictions)
print('A Decision Tree algorithm had an accuracy of: {0:2.2f}%'.format(dtc_acc*100))