from pyspark.ml.feature import CountVectorizer
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.sql import SQLContext
import warnings
import pyspark as ps
import findspark
findspark.init()

try:
    # create SparkContext on all CPUs available: in my case I have 4 CPUs on my laptop
    sc = ps.SparkContext('local[4]')
    sqlContext = SQLContext(sc)
    print("Just created a SparkContext")
except ValueError:
    warnings.warn("SparkContext already exists in this scope")


df = sqlContext.read.format('com.databricks.spark.csv').options(
    header='true', inferschema='true').load('./datasets/clean_tweet.csv')
df = df.dropna()
df.count()

(train_set, val_set, test_set) = df.randomSplit([0.98, 0.01, 0.01], seed=2000)

tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashtf = HashingTF(numFeatures=2**16, inputCol="words", outputCol='tf')
# minDocFreq: remove sparse terms
idf = IDF(inputCol='tf', outputCol="features", minDocFreq=5)
label_stringIdx = StringIndexer(inputCol="target", outputCol="label")
pipeline = Pipeline(stages=[tokenizer, hashtf, idf, label_stringIdx])

pipelineFit = pipeline.fit(train_set)
train_df = pipelineFit.transform(train_set)
val_df = pipelineFit.transform(val_set)
train_df.show(5)
lr = LogisticRegression(maxIter=100)
lrModel = lr.fit(train_df)
predictions = lrModel.transform(val_df)
evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
print("EVALUATOR", evaluator.evaluate(predictions))

accuracy = predictions.filter(
    predictions.label == predictions.prediction).count() / float(val_set.count())
print("ACCURACY", accuracy)


# tokenizer = Tokenizer(inputCol="text", outputCol="words")
# cv = CountVectorizer(vocabSize=2**16, inputCol="words", outputCol='cv')
# # minDocFreq: remove sparse terms
# idf = IDF(inputCol='cv', outputCol="features", minDocFreq=5)
# label_stringIdx = StringIndexer(inputCol="target", outputCol="label")
# lr = LogisticRegression(maxIter=100)
# pipeline = Pipeline(stages=[tokenizer, cv, idf, label_stringIdx, lr])

# pipelineFit = pipeline.fit(train_set)
# predictions = pipelineFit.transform(val_set)
# accuracy = predictions.filter(
#     predictions.label == predictions.prediction).count() / float(val_set.count())
# roc_auc = evaluator.evaluate(predictions)

# print("Accuracy Score: {0:.4f}".format(accuracy))
# print("ROC-AUC: {0:.4f}".format(roc_auc))
