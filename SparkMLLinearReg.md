# Synapse Analytics Using Spark based Machine learning - Linear Regression

Unified Analytics Tool to ingest, compute or process data, Store data, Advanced analytics or machine learning and Display all in one tool. End to end data analytics platform built to scale and ease of use. 

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/synapseprocess.JPG "Synapse Analytics")

## Syanpse Advanced Analaytics

Synapse has the ability to run spark based code which leads to Data engineering or feature engineering and also Machine learning. This articles describes how to train a machine learning model using spark in synapse.

## Prerequiste

- Prepare Data set
Download the training data set. Training dataset is newyork city taxi data set which is avaialble for public.
here is my copy available in case:
https://waginput.blob.core.windows.net/incoming/train.csv

- Move/copy data into blob storage or ADLS gen2

Upload the above training file to blob storage or ADLS Gen2. Or you can use synapse orchestrate feature to move the data into Blob.

```
For my testing i was able to move the blob storage train.csv into ADLS gen2 filesystem. I did that for just to show how to move data inside synapse analytics.
```

- Build machine learning model using spark scala

First lets connect to the data storage to get the data

This is to get the spark version running.
```
%%pyspark
import pyspark 
print(print(pyspark.__version__)) 
```
Now Lets configure the blob storage to get data:
```
spark.conf.set(
  "fs.azure.account.key.waginput.blob.core.windows.net",
  "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
```

Read the csv file into a data frame
```
val df = spark.read.option("header","true").option("inferSchema","true").csv("wasbs://incoming@waginput.blob.core.windows.net/train.csv")
```

Print the schema:
```
df.printSchema
```

Set the features list for Machine learning modelling:
```
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
val featureCols=Array("fare_amount","pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude","passenger_count")
val assembler: org.apache.spark.ml.feature.VectorAssembler= new VectorAssembler().setInputCols(featureCols).setOutputCol("features")

val assembledDF = assembler.setHandleInvalid("skip").transform(df)
val assembledFinalDF = assembledDF.select("fare_amount","features")
```

Normalize the dataframe:
```
import org.apache.spark.ml.feature.Normalizer

val normalizedDF = new Normalizer().setInputCol("features").setOutputCol("normalizedFeatures").transform(assembledFinalDF)
```

Drop missing data point in the data frame:
```
val normalizedDF1 = normalizedDF.na.drop()
```

Now split the data set for training and test. 70% for training and 30% for testing
```
val Array(trainingDS, testDS) = normalizedDF1.randomSplit(Array(0.7, 0.3))
```

Build the linear regression model and execute it:
```
import org.apache.spark.ml.regression.LinearRegression
// Create a LinearRegression instance. This instance is an Estimator.
val lr = new LinearRegression().setLabelCol("fare_amount").setMaxIter(100)
// Print out the parameters, documentation, and any default values.
println(s"Linear Regression parameters:\n ${lr.explainParams()}\n")
// Learn a Linear Regression model. This uses the parameters stored in lr.
val lrModel = lr.fit(trainingDS)
// Make predictions on test data using the Transformer.transform() method.
// LinearRegression.transform will only use the 'features' column.
val lrPredictions = lrModel.transform(testDS)
```

Test the model with test data:
```
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
println("\nPredictions : " )
lrPredictions.select($"fare_amount".cast(IntegerType),$"prediction".cast(IntegerType)).orderBy(abs($"prediction"-$"fare_amount")).distinct.show(15)
```

Now time to evalute the model:
```
import org.apache.spark.ml.evaluation.RegressionEvaluator
val evaluator_r2 = new RegressionEvaluator().setPredictionCol("prediction").setLabelCol("fare_amount").setMetricName("r2")

//As the name implies, isLargerBetter returns if a larger value is better or smaller for evaluation.
val isLargerBetter : Boolean = evaluator_r2.isLargerBetter
println("Coefficient of determination = " + evaluator_r2.evaluate(lrPredictions))
```

evaulate the above models output:
```
//Evaluate the results. Calculate Root Mean Square Error
val evaluator_rmse = new RegressionEvaluator().setPredictionCol("prediction").setLabelCol("fare_amount").setMetricName("rmse")
//As the name implies, isLargerBetter returns if a larger value is better for evaluation.
val isLargerBetter1 : Boolean = evaluator_rmse.isLargerBetter
println("Root Mean Square Error = " + evaluator_rmse.evaluate(lrPredictions))
```

Model was successfully built and executed. Now next is to create inference code and build a inferenceing process for production
