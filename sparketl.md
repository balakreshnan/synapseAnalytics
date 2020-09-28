# Azure Spark ETL in Azure Synapse Analtyics (Workspace) with sample datasets

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

More to come.