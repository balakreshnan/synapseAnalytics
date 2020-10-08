# Azure Synapse SQL on demand NYC data set demo

## Load open source NYC taxi data set and do query processing

## PySpark code to save the data

- Need to copy the data from open data set into local synapse default storage
- Read Yellow taxi data
- Create a notebook with pyspark as language

```
blob_account_name = "azureopendatastorage"
blob_container_name = "nyctlc"
blob_relative_path = "yellow"
blob_sas_token = r""

# Allow Spark to read from Blob remotely
wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name),blob_sas_token)

# Spark read parquet, note that it won't load any data yet by now
yellowdf = spark.read.parquet(wasbs_path)
```

- display and validate the data set

```
display(yellowdf)
```

- Read Green Taxi details and display

```
blob_account_name = "azureopendatastorage"
blob_container_name = "nyctlc"
blob_relative_path = "green"
blob_sas_token = r""

# Allow Spark to read from Blob remotely
wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name),blob_sas_token)

# Spark read parquet, note that it won't load any data yet by now
greendf = spark.read.parquet(wasbs_path)
```

- display and validate the data set

```
display(greendf)
```

- import pyspark necessary libraries

```
import org.apache.spark.sql
```

- Time to writ the yellow dataset 

```
yellowdf.write.mode('overwrite').parquet("/nyctaxiyellow")
```

- Time to write the green data set to local storage

```
greendf.write.mode('overwrite').parquet("/nyctaxigreen")
```

- Now the data is loaded let's go and write queries using sql on demand
- Go to Develop and open new query windows
- Load the data first

```
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK     'https://accsynapsestorage.blob.core.windows.net/synapseroot/nyctaxiyellow/*',
        FORMAT = 'parquet'
    ) AS [result];
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/sqlondeman1.jpg "ETL")

```
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK     'https://accsynapsestorage.blob.core.windows.net/synapseroot/nyctaxigreen/*',
        FORMAT = 'parquet'
    ) AS [result];
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/sqlondeman2.jpg "ETL")

- Let's count the records 

```
SELECT
    count(*)
FROM
    OPENROWSET(
        BULK     'https://accsynapsestorage.blob.core.windows.net/synapseroot/nyctaxiyellow/*',
        FORMAT = 'parquet'
    ) AS [result];
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/sqlondeman3.jpg "ETL")

- count

```
SELECT
    count(*)
FROM
    OPENROWSET(
        BULK     'https://accsynapsestorage.blob.core.windows.net/synapseroot/nyctaxigreen/*',
        FORMAT = 'parquet'
    ) AS [result];
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/sqlondeman4.jpg "ETL")

- Write ETL queries

```
SELECT
    PuYear, PuMonth, sum(FareAmount) as FareAmount, sum(TotalAmount) as TotalAmount
FROM
    OPENROWSET(
        BULK     'https://accsynapsestorage.blob.core.windows.net/synapseroot/nyctaxiyellow/*',
        FORMAT = 'parquet'
    ) AS [result] 
    Group by PuYear, PuMonth
    Order by PuYear, PuMonth;
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/sqlondeman5.jpg "ETL")