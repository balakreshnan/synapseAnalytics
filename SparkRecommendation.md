# Azure Synapse Analytics Run Recommendation model

Run Recommendation supervised learning with Movie data set. 

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/synapseprocess.JPG "Synapse Analytics")

## Syanpse Advanced Analytics

Synapse has the ability to run spark based code which leads to Data engineering or feature engineering and also Machine learning. This articles describes how to train a machine learning model using spark in synapse.

## Prerequiste

- Prepare Data set
Download the training data set. Training dataset is newyork city taxi data set which is avaialble for public.
here is my copy available in case:
https://github.com/balakreshnan/synapseAnalytics/tree/master/datasets/movies

- Move/copy data into blob storage or ADLS gen2

Upload the above training file to blob storage or ADLS Gen2. Or you can use synapse orchestrate feature to move the data into Blob.

```
For my testing i was able to move the blob storage train.csv into ADLS gen2 filesystem. I did that for just to show how to move data inside synapse analytics.
```

## imports

```
import org.apache.spark.sql.SQLContext
//val sqlContext = new SQLContext(sc);
import org.apache.spark.sql.types._
//https://docs.microsoft.com/en-us/azure/databricks/_static/notebooks/movie-recommendation-engine.html
```

## Load and Process Data

## Process Movies data set

Read the dat file with delimiter "::". Since delimiter is single character i am using ":"
```
val moviesdf = spark.read.format("csv").option("header", "false").option("inferSchema", "true").option("delimiter", ":").load("abfss://dataset@storageaccountname.dfs.core.windows.net/movies/movies.dat")
```

Now time to onlt select necessary columns and rename to proper names.

```
val moviesdf1 = moviesdf.select("_c0","_c2","_c4").withColumnRenamed("_c0","MovieID").withColumnRenamed("_c2","MovieName").withColumnRenamed("_c4","Genre")
```

Validate the dataset.

```
display(moviesdf1)
```

## Process Ratings data set

Read the dat file with delimiter "::". Since delimiter is single character i am using ":"
```
val ratingsdf = spark.read.format("csv").option("header", "false").option("delimiter", ":").load("abfss://dataset@storageaccountname.dfs.core.windows.net/movies/ratings.dat")
```

Now time to onlt select necessary columns and rename to proper names.

```
val ratingsdf1 = ratingsdf.select("_c0","_c2","_c4","_c6").withColumnRenamed("_c0","UserID").withColumnRenamed("_c2","MovieID").withColumnRenamed("_c4","Rating").withColumnRenamed("_c6","Timestamp")
```

Validate the dataset.

```
display(ratingsdf1)
```

## Process Users data set

Read the dat file with delimiter "::". Since delimiter is single character i am using ":"
```
val usersdf = spark.read.format("csv").option("header", "false").option("delimiter", ":").load("abfss://dataset@storageaccountname.dfs.core.windows.net/movies/users.dat")
```

Now time to onlt select necessary columns and rename to proper names.

```
val usersdf1 = usersdf.select("_c0","_c2","_c4","_c6","_c8").withColumnRenamed("_c0","UserID").withColumnRenamed("_c2","Gender").withColumnRenamed("_c4","Age").withColumnRenamed("_c6","Occupation").withColumnRenamed("_c8","ZipCode")
```

Validate the dataset.

```
display(usersdf1)
```

## Spark ALS model building

First part is to train the model

Import the necessary includes for ALS recommendation model

```
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
```

```
ratingsdf1.withColumn("UserID1", ratingsdf1.UserID.cast(IntegerType)).withColumn("MovieID1", ratingsdf1.MovieID.cast(IntegerType)).withColumn("Rating1", ratingsdf1.Rating.cast(IntegerType))
```

Convert the columns to integer or double to process

```
val df2 = ratingsdf1.selectExpr("cast(UserID as int) UserID", "cast(MovieID as int) MovieID", "cast(Rating as int) Rating")
```

Split the data set for training and test

```
val Array(training, test) = df2.randomSplit(Array(0.8, 0.2))
```

Time to train the model with training data set. Specify the UserID, MovieID and Ratings columns that are needed for recommendation.

```
// Build the recommendation model using ALS on the training data
val als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("UserID").setItemCol("MovieID").setRatingCol("Rating")
val model = als.fit(training)
```

```
// Evaluate the model by computing the RMSE on the test data
// Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
model.setColdStartStrategy("drop")
val predictions = model.transform(test)
```

Evaulate the model

```
val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("Rating").setPredictionCol("prediction")
val rmse = evaluator.evaluate(predictions)
println(s"Root-mean-square error = $rmse")
```

```
// Generate top 10 movie recommendations for each user
val userRecs = model.recommendForAllUsers(10)
// Generate top 10 user recommendations for each movie
val movieRecs = model.recommendForAllItems(10)
```
Have fun.