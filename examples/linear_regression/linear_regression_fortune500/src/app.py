from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import regexp_replace
from pyspark.ml.regression import LinearRegression


## Supporting Functions


## Core Spark Application Functionality

def main(sqlContext):
    company_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(
        './linear_regression_fortune500/src/inputData/Fortune_500.csv')
    company_df = company_df.withColumn('Number of Employees', regexp_replace('Number of Employees', ',', ''))
    company_df = company_df.withColumn('Number of Employees', company_df['Number of Employees'].cast("int"))
    company_df.show(10)

    company_df.cache()
    company_df.printSchema()

    company_df.describe().toPandas().transpose()

    vectorAssembler = VectorAssembler(inputCols=['Rank', 'Number of Employees'], outputCol='features')
    tcompany_df = vectorAssembler.setHandleInvalid("keep").transform(company_df)
    tcompany_df = tcompany_df.select(['features', 'Number of Employees'])
    tcompany_df.show(10)

    splits = tcompany_df.randomSplit([0.7, 0.3])
    train_df = splits[0]
    test_df = splits[1]

    lr = LinearRegression(featuresCol='features', labelCol='Number of Employees', maxIter=10, regParam=0.3,
                          elasticNetParam=0.8)
    lr_model = lr.fit(train_df)
    print("Coefficients: " + str(lr_model.coefficients))
    print("Intercept: " + str(lr_model.intercept))

    trainingSummary = lr_model.summary
    print("RMSE on training data: %f" % trainingSummary.rootMeanSquaredError)
    print("r2 on training data: %f" % trainingSummary.r2)

    train_df.describe().show()

    lr_predictions = lr_model.transform(test_df)
    lr_predictions.select("prediction", "Number of Employees", "features").show()

    test_result = lr_model.evaluate(test_df)
    print("Root Mean Squared Error (RMSE) on test data = %g" % test_result.rootMeanSquaredError)

    print("numIterations: %d" % trainingSummary.totalIterations)
    print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
    trainingSummary.residuals.show()

if __name__ == "__main__":
    # Configure Spark Application
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    # Execute Main functionality
    main(sqlContext)