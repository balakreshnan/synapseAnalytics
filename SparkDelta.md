# Azure synapse analytics spark processing delta lake

Implement Delta Lake in Azure Synapse Analytics Spark.

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/synapseprocess.JPG "Synapse Analytics")

## Load data

Time to configure the blob storage and load the data

```
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
```

```
spark.conf.set(
  "fs.azure.account.key.waginput.blob.core.windows.net",
  "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
```

```
val df = spark.read.option("header","true").option("inferSchema","true").csv("wasbs://incoming@waginput.blob.core.windows.net/train.csv")
display(df)
```

## Time to create parition column

```
import org.apache.spark.sql.SaveMode
```

```
val df1 = df.withColumn("Date", (col("pickup_datetime").cast("date")))
display(df1)
```

## write to delta lake

```
df1.write.format("delta").mode("overwrite").partitionBy("Date").save("/delta/taxidata/")
```

## Read from delta lake

```
val df_delta = spark.read.format("delta").load("/delta/taxidata/")
display(df_delta)
```

## spark sql syntax for delta lake loading and truncating

```
display(spark.sql("DROP TABLE IF EXISTS taxidata"))
        
display(spark.sql("CREATE TABLE taxidata USING DELTA LOCATION '/delta/taxidata/'"))
```

```
display(spark.sql("TRUNCATE TABLE taxidata"))
```

## check the loaded data count

```
df_delta.count()
```

## loading different data set for merge operation

```
val historical_events = spark.read.option("header","true").option("inferSchema","true").csv("wasbs://incoming@waginput.blob.core.windows.net/train.csv")
```

```
val historical_events1 = df.withColumn("Date", (col("pickup_datetime").cast("date")))
```

## invoke Delta lake merge

```
import io.delta.tables._
import org.apache.spark.sql.functions._
```

```
DeltaTable.forPath(spark, "/delta/taxidata/").as("org").merge(historical_events1.as("updates"),"org.key = updates.key and org.pickup_datetime = updates.pickup_datetime and org.pickup_longitude = updates.pickup_longitude and org.pickup_latitude = updates.pickup_latitude and org.dropoff_longitude = updates.dropoff_longitude and org.dropoff_latitude = updates.dropoff_latitude").whenNotMatched().insertAll().execute()
```

## Validate the merge

```
df_delta.count()
```

## alertanate merge using append

```
historical_events1.write.format("delta").mode("append").partitionBy("Date").save("/delta/taxidata/")
```
