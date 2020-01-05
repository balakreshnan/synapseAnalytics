# Synapse Analytics Using Spark based Machine learning - Linear Regression

Unified Analytics Tool to ingest, compute or process data, Store data, Advanced analytics or machine learning and Display all in one tool. End to end data analytics platform built to scale and ease of use. 

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

## Split data for training and testing

```
val Array(trainingData, testData) = normalizedDF1.randomSplit(Array(0.7, 0.3))
```


## inlcudes for modelling

```
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
```

```
// Index labels, adding metadata to the label column.
// Fit on whole dataset to include all labels in index.
val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(normalizedDF1)
```

```
// Automatically identify categorical features, and index them.
// Set maxCategories so features with > 4 distinct values are treated as continuous.
val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(normalizedDF1)
```

Set the GBT model parameters and inputs

```
// Train a GBT model.
val gbt = new GBTClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setMaxIter(10)
```

Convert label back

```
// Convert indexed labels back to original labels.
val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
```

Create the pipeline to run the model training

```
// Chain indexers and GBT in a Pipeline.
val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))
```

```
// Train model. This also runs the indexers.
val model = pipeline.fit(trainingData)
```

```
// Make predictions.
val predictions = model.transform(testData)
```

Print the top 5 prediction

```
// Select example rows to display.
predictions.select("predictedLabel", "label", "features").show(5)
```

run the evalutor.

```
// Select (prediction, true label) and compute test error.
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
val accuracy = evaluator.evaluate(predictions)
println("Test Error = " + (1.0 - accuracy))

val gbtModel = model.stages(2).asInstanceOf[GBTClassificationModel]
println("Learned classification GBT model:\n" + gbtModel.toDebugString)
```