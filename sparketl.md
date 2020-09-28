# Azure Spark ETL in Azure Synapse Analtyics (Workspace) with sample datasets and machine learning

## Use existing data sets to load and process data to learn ETL - Extract, Transform and Load

## Use Case

Ability to process data using sample data sets to learn Extract, Transform and Load in scale using Azure synapse analytics workspace spark. 

- Using Pyspark

## Pre-requsitie

- Azure account
- Create Azure Synapse analytics workspace
- Create spark spools
- I am using medium instance
- No libraries are uploaded

## Steps to ETL

- Load the data from samples datasets

```
from azureml.opendatasets import NycTlcYellow

data = NycTlcYellow()
data_df = data.to_spark_dataframe()
# Display 10 rows
display(data_df.limit(10))
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/etl0.jpg "ETL")

- Display the data

```
display(data_df)
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/etl1.jpg "ETL")

- Bring the imports

```
from pyspark.sql.functions import *
from pyspark.sql import *
```

- Create a date column which allows us to go aggregation which is very common use case for ETL

```
df1 = data_df.withColumn("Date", (col("tpepPickupDateTime").cast("date"))) 
display(df1)
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/etl2.jpg "ETL")

- Drop duplicates if necessary

```
df1.dropDuplicates("key","pickup_datetime","pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude")
```

- Display the schema

```
df1.printSchema
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/etl3.jpg "ETL")

- Create year, month and day columns to make it easier for data set to do aggregation

```
df2 = df1.withColumn("year", year(col("date"))) .withColumn("month", month(col("date"))) .withColumn("day", dayofmonth(col("date"))) .withColumn("hour", hour(col("date")))
display(df2)
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/etl4.jpg "ETL")

- Now lets do aggregation using group by

```
df2.groupBy("year","month").agg(sum("fareAmount").alias("Total"),count("vendorID").alias("Count")).sort(asc("year"), asc("month")).show()
```

- Save the group by to another dataset

```
dfgrouped = df2.groupBy("year","month").agg(sum("fareAmount").alias("Total"),count("vendorID").alias("Count")).sort(asc("year"), asc("month"))
display(dfgrouped)
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/etl5.jpg "ETL")

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/etl6.jpg "ETL")

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/etl7.jpg "ETL")

- now lets see if we can do the same aggregation using spark sql

```
df2.createOrReplaceTempView("nycyellow")
```

```
%%sql
select  year(cast(tpepPickupDateTime  as timestamp)) as tsYear,
        day(cast(tpepPickupDateTime  as timestamp)) as tsDay, 
        hour(cast(tpepPickupDateTime  as timestamp)) as tsHour,
        avg(totalAmount) as avgTotal, avg(fareAmount) as avgFare
from nycyellow
group by  tsYear,tsDay, tsHour
order by  tsYear,tsDay, tsHour
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/etl8.jpg "ETL")

- Let's create a Regression model to predict hourly what would be the Total Avg for forecasting

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/etl9.jpg "ETL")

## Machine learning (regression)

- Create a new Table called dailyaggr and save the aggregated data

```
%%sql
CREATE TABLE dailyaggr
  COMMENT 'This table is created with existing data'
  AS select  year(cast(tpepPickupDateTime  as timestamp)) as tsYear,
        month(cast(tpepPickupDateTime  as timestamp)) as tsmonth,
        day(cast(tpepPickupDateTime  as timestamp)) as tsDay, 
        hour(cast(tpepPickupDateTime  as timestamp)) as tsHour,
        avg(totalAmount) as avgTotal, avg(fareAmount) as avgFare
from nycyellow
group by  tsYear, tsmonth,tsDay, tsHour
order by  tsYear, tsmonth,tsDay, tsHour
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/etl10.jpg "ETL")

- include linear regression libraries

```
from pyspark.ml.regression import LinearRegression
```

- here i am also switching between scala now to regression modelling
- Majic command is %%spark for scala programming

```
%%spark
import org.apache.spark.ml.feature.VectorAssembler 
import org.apache.spark.ml.linalg.Vectors 
val dailyaggr = spark.sql("SELECT tsYear, tsMonth, tsDay, tsHour, avgTotal FROM dailyaggr")
val featureCols=Array("tsYear","tsMonth","tsDay","tsHour") 
val assembler: org.apache.spark.ml.feature.VectorAssembler= new VectorAssembler().setInputCols(featureCols).setOutputCol("features") 
val assembledDF = assembler.setHandleInvalid("skip").transform(dailyaggr) 
val assembledFinalDF = assembledDF.select("avgTotal","features")
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/etl11.jpg "ETL")

- create features

```
%%spark
import org.apache.spark.ml.feature.Normalizer 
val normalizedDF = new Normalizer().setInputCol("features").setOutputCol("normalizedFeatures").transform(assembledFinalDF)
```

- drop duplicates

```
%%spark
val normalizedDF1 = normalizedDF.na.drop()
```

- Split 70% for training and 30% testing

```
%%spark
val Array(trainingDS, testDS) = normalizedDF1.randomSplit(Array(0.7, 0.3))
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/etl12.jpg "ETL")

- Create the model and it's parameter

```
%%spark
import org.apache.spark.ml.regression.LinearRegression
// Create a LinearRegression instance. This instance is an Estimator. 
val lr = new LinearRegression().setLabelCol("avgTotal").setMaxIter(100)
// Print out the parameters, documentation, and any default values. println(s"Linear Regression parameters:\n ${lr.explainParams()}\n") 
// Learn a Linear Regression model. This uses the parameters stored in lr.
val lrModel = lr.fit(trainingDS)
// Make predictions on test data using the Transformer.transform() method.
// LinearRegression.transform will only use the 'features' column. 
val lrPredictions = lrModel.transform(testDS)
```

```
%%spark
import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.types._ 
println("\nPredictions : " ) 
lrPredictions.select($"avgTotal".cast(IntegerType),$"prediction".cast(IntegerType)).orderBy(abs($"prediction"-$"avgTotal")).distinct.show(15)
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/etl13.jpg "ETL")

- Evaulate the mode

```
%%spark
import org.apache.spark.ml.evaluation.RegressionEvaluator 

val evaluator_r2 = new RegressionEvaluator().setPredictionCol("prediction").setLabelCol("avgTotal").setMetricName("r2") 
//As the name implies, isLargerBetter returns if a larger value is better or smaller for evaluation. 
val isLargerBetter : Boolean = evaluator_r2.isLargerBetter 
println("Coefficient of determination = " + evaluator_r2.evaluate(lrPredictions))
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/etl14.jpg "ETL")

- Evaulate the RMSE 

```
%%spark
//Evaluate the results. Calculate Root Mean Square Error 
val evaluator_rmse = new RegressionEvaluator().setPredictionCol("prediction").setLabelCol("avgTotal").setMetricName("rmse") 
//As the name implies, isLargerBetter returns if a larger value is better for evaluation. 
val isLargerBetter1 : Boolean = evaluator_rmse.isLargerBetter 
println("Root Mean Square Error = " + evaluator_rmse.evaluate(lrPredictions))
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/etl15.jpg "ETL")

More to come.