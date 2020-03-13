from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import FloatType
from pyspark.ml.regression import LinearRegression

import six

sc = SparkContext()
sqlContext = SQLContext(sc)

company_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('Fortune_500.csv')
company_df = company_df.withColumn('Number of Employees', regexp_replace('Number of Employees', ',', ''))
company_df = company_df.withColumn('Number of Employees', company_df['Number of Employees'].cast("int"))
company_df.show(10)

company_df.cache()
company_df.printSchema()

company_df.describe().toPandas().transpose()

# company_df = company_df.withColumn("Number of Employees",company_df["Number of Employees"].cast(IntegerType()))

company_df.cache()
company_df.printSchema()

# for i in company_df.columns:
#     if not( isinstance(company_df.select(i).take(1)[0][0], six.string_types)):
#         print( "Correlation to Employees for ", i, company_df.stat.corr('Number of Employees',i))
company_df.show(10)

vectorAssembler = VectorAssembler(inputCols = ['Rank', 'Number of Employees'], outputCol = 'features')
tcompany_df = vectorAssembler.setHandleInvalid("keep").transform(company_df)
tcompany_df = tcompany_df.select(['features', 'Number of Employees'])
tcompany_df.show(10)

splits = tcompany_df.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]

lr = LinearRegression(featuresCol = 'features', labelCol='Number of Employees', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_df)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))