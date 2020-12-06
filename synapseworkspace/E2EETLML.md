# Azure Synapse Analytics End to End Machine learning

## Load, tranform, Model, Store Data

## Use Case

- Load data from data source in this case sample dataset
- Process data (ETL) using Pyspark
- ETL work is done in combination with pyspark dataframe and Spark SQL
- Save processed data into Synapse dedicated SQLPools
- Resume and Pause dedicated SQL pools as we run ETL
- Save a copy in default storage for serverless activities
- Build and Train Machine learning model
- We are using same notebook as pyspark but building with scala code
- Build different Pipeline for Resuming and Pausing dedicated SQL Pools

Note:
```
Not every use case you have to resume and pause dedicated sql pools. If needed use, other wise please do ignore. The idea for this tutorial is to show all the combination of features working together.
```

## End to End Processing Architecture

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/e2etest1.jpg "Synapse Analytics")

## Prerequisite

- Need Azure account
- Create a resource group
- Create Azure Synapse Analytics workspace
- Create a dedicated SQL pools ( i am using DW100c)
- Create a Spark Spool
- Server less sql will automatically be created

## Steps to Create the code

- First Create notebook code
- Then create Resume pipeline for Dedicated SQL Pools
- Create Pause pipeline for dedicated SQL pools
- create another pipeline to run resume, notebook and then pause

## Steps to create Resume integration

- Create a new pipe line
- Drag Web Activity from general section
- Go to Settings and for URL

```
https://management.azure.com/subscriptions/subid/resourceGroups/rggroupname/providers/Microsoft.Synapse/workspaces/workspacename/sqlPools/poolname/resume?api-version=2019-06-01-preview
```

- now for body 

```
{"sku":{"name":"DW100c"}}
```

- Select Method as POST
- For Authentication section on the resource type

```
https://management.azure.com/
```

## Steps to create Pause integration

- Create a new pipe line
- Drag Web Activity from general section
- Go to Settings and for URL

```
https://management.azure.com/subscriptions/subid/resourceGroups/rggroupname/providers/Microsoft.Synapse/workspaces/workspacename/sqlPools/poolname/pause?api-version=2019-06-01-preview
```

- now for body 

```
{"sku":{"name":"DW100c"}}
```

- Select Method as POST
- For Authentication section on the resource type

```
https://management.azure.com/
```

## Code to create ETL and ML Code using Notebook

- Let's load the data set

```
from azureml.opendatasets import NycTlcYellow

data = NycTlcYellow()
data_df = data.to_spark_dataframe()
# Display 10 rows
display(data_df.limit(10))
```

```
from pyspark.sql.functions import *
from pyspark.sql import *
```

- Create a data column

```
df1 = data_df.withColumn("Date", (col("tpepPickupDateTime").cast("date"))) 
display(df1)
```

- Create a View to use later

```
df1.createOrReplaceTempView("nycyellow")
```

- find the total count of records

```
#df1.count()
```

- Drop duplicates

```
#df1.dropDuplicates("key","pickup_datetime","pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude")
```

- Print schema

```
df1.printSchema
```

- Create new columns from derived Date column

```
df2 = df1.withColumn("year", year(col("date"))) .withColumn("month", month(col("date"))) .withColumn("day", dayofmonth(col("date"))) .withColumn("hour", hour(col("date")))
```

- Now group the data

```
dfgrouped = df2.groupBy("year","month").agg(sum("fareAmount").alias("Total"),count("vendorID").alias("Count")).sort(asc("year"), asc("month"))
```

- Display

```
display(dfgrouped)
```

- Lets write the data to underlying storage

```
dfgrouped.repartition(1).write.option("header","true").csv("/dailyaggrcsv/csv/dailyaggr.csv")
```

- Now lets try the same aggregation using Spark SQL

```
df2.createOrReplaceTempView("nycyellow")
```

- Let's display the data and validate the aggregation doesn't exist

```
%%sql
select * from nycyellow limit 100
```

- Aggregate records based on year, month, day and hour

```
%%sql
select  year(cast(tpepPickupDateTime  as timestamp)) as tsYear,
        month(cast(tpepPickupDateTime  as timestamp)) as tsmonth,
        day(cast(tpepPickupDateTime  as timestamp)) as tsDay, 
        hour(cast(tpepPickupDateTime  as timestamp)) as tsHour,
        avg(totalAmount) as avgTotal, avg(fareAmount) as avgFare
from nycyellow
group by  tsYear, tsmonth,tsDay, tsHour
order by  tsYear, tsmonth,tsDay, tsHour
```

- drop if the aggrtable exist

```
%%sql
DROP TABLE dailyaggr
```

- Create the aggregate table

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

- Validate records are stored

```
%%sql
select * from dailyaggr
```

### Load the aggregated data into dedicated sql pools

- load the spark sql table data to dataframe

```
dailyaggr = spark.sql("SELECT tsYear, tsMonth, tsDay, tsHour, avgTotal FROM dailyaggr")
```

- display the dataset to confirm

```
display(dailyaggr)
```

- Write to dedicated SQL Pools
- as you see sqlanalytics is inbuilt in the synapse workspace for native integration

```
dailyaggr.write.sqlanalytics("accsynapsepools.wwi.dailyaggr", Constants.INTERNAL)
```

### Time to build machine learning Model

### Model

```
from pyspark.ml.regression import LinearRegression
```

- Let's load the data
- From now we are using Scala code

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

```
%%spark
import com.microsoft.spark.sqlanalytics.utils.Constants
import org.apache.spark.sql.SqlAnalyticsConnector._
```

```
%%spark
import org.apache.spark.ml.feature.Normalizer 
val normalizedDF = new Normalizer().setInputCol("features").setOutputCol("normalizedFeatures").transform(assembledFinalDF)
```

- Drop NA

```
%%spark
val normalizedDF1 = normalizedDF.na.drop()
```

- Split data for training and testing
- we split 70% data for training
- 30% data for testing

```
%%spark
val Array(trainingDS, testDS) = normalizedDF1.randomSplit(Array(0.7, 0.3))
```

- Train the model

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

- Predict the data

```
%%spark
import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.types._ 
println("\nPredictions : " ) 
lrPredictions.select($"avgTotal".cast(IntegerType),$"prediction".cast(IntegerType)).orderBy(abs($"prediction"-$"avgTotal")).distinct.show(15)
```

- Evaluate the model to make sure it is valid

```
%%spark
import org.apache.spark.ml.evaluation.RegressionEvaluator 

val evaluator_r2 = new RegressionEvaluator().setPredictionCol("prediction").setLabelCol("avgTotal").setMetricName("r2") 
//As the name implies, isLargerBetter returns if a larger value is better or smaller for evaluation. 
val isLargerBetter : Boolean = evaluator_r2.isLargerBetter 
println("Coefficient of determination = " + evaluator_r2.evaluate(lrPredictions))
```

- Print the Metric

```
%%spark
//Evaluate the results. Calculate Root Mean Square Error 
val evaluator_rmse = new RegressionEvaluator().setPredictionCol("prediction").setLabelCol("avgTotal").setMetricName("rmse") 
//As the name implies, isLargerBetter returns if a larger value is better for evaluation. 
val isLargerBetter1 : Boolean = evaluator_rmse.isLargerBetter 
println("Root Mean Square Error = " + evaluator_rmse.evaluate(lrPredictions))
```

- Modelling is completed now.
- Save the notebook
- click commit all.
- Run the cells one by one and observe the output.

## Create the End to end integration

- Now that we have the resume, pause integration pipline and also the ETL and ML modeling code ready.
- Let's create the integration pipeline to combine all of them like below.

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/e2etest1.jpg "Synapse Analytics")

- Create a new integration
- First select execute pipeline and select Resume dedicated SQL pools pipeline
- Next drag the Synapse notebook job and select the above notebook created
- final step drag execute pipeline and select Pause dedicated SQL pools pipeline
- Save all and commit
- Once it saved create a pull request and merge with main branch
- Switch to main branch
- Click Publish
- Click Trigger now
- Wait and see how the pipeline execute.