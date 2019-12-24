# Azure Synapse Analytics Run Clustering model

Run K means clustering unsupervised learning with taxi data set. 

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/synapseprocess.JPG "Synapse Analytics")

## Syanpse Advanced Analytics

Synapse has the ability to run spark based code which leads to Data engineering or feature engineering and also Machine learning. This articles describes how to train a machine learning model using spark in synapse.

## Prerequiste

- Prepare Data set
Download the training data set. Training dataset is newyork city taxi data set which is avaialble for public.
here is my copy available in case:
https://dewsa.blob.core.windows.net/taxidata/train.csv

- Move/copy data into blob storage or ADLS gen2

Upload the above training file to blob storage or ADLS Gen2. Or you can use synapse orchestrate feature to move the data into Blob.

```
For my testing i was able to move the blob storage train.csv into ADLS gen2 filesystem. I did that for just to show how to move data inside synapse analytics.
```

## Imports 

```
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import spark.implicits._
import org.apache.spark.sql.functions._
```

## Load data

```
val yellowdf = spark.read.option("header","true").option("inferSchema","true").parquet("abfss://opendataset@internalsandboxwe.dfs.core.windows.net/nyctlc/yellow/")
```

Validate the dataset

```
display(yellowdf)
```

See the schema

```
yellowdf.schema
```

## Select only columns needed for clustering 

(preferably numeric values)

```
val df = yellowdf.select("passengerCount","tripDistance","fareAmount","tipAmount","totalAmount","puYear","puMonth")
```

Validate the new dataset

```
display(df)
```

Make sure only selected columns are in the New data set

```
df.schema
```

## Format the data set for model run

Cnovert the dataframe into vector format so that kmeans can process.

```
val assembler = new VectorAssembler().setInputCols(Array("passengerCount","tripDistance","fareAmount","tipAmount","totalAmount","puYear","puMonth")).setOutputCol("features").setOutputCol("features")

val training = assembler.transform(df.na.drop())
```

## Run the K means cluster model training

```
// Cluster the data into two classes using KMeans
val numClusters = 2
val numIterations = 20
// Trains a k-means model.
val kmeans = new KMeans().setK(numClusters).setSeed(1L).setFeaturesCol("features").setPredictionCol("prediction")
val model = kmeans.fit(training)
```

## Validate the model

```
// Evaluate clustering by computing Within Set Sum of Squared Errors.
val WSSSE = model.computeCost(training)
println(s"Within Set Sum of Squared Errors = $WSSSE")
```

Run preditions

```
val predicted = model.transform(training)
predicted.show 
```

Show results:

```
// Shows the result
println("Final Centers: ")
model.clusterCenters.foreach(println)
```