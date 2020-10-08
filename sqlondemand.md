# Azure Synapse SQL-ondemand

## How to use Azure Synapse SQL on demand and do ETL

## Use Case

Build a system to load covid 19 data which is available in kagal web site. 

## Steps

- Create a Azure Syanpse workspace resource
- Make sure permission for Managed instance is provided with storage blob container owner
- Go to Kaggal web site and download covid 19 data set
- sample file is also available in data folder with file name covid_19_data
- Create a container called coviddata in the Default storage
- Upload the file using portal or Storage explorer (Available online or local install)
- Go to workspace 
- Create a new query in Develop section
- First we need to create a database if not available

```
CREATE DATABASE coviddb;
```

- Lets load sample data and make sure if it works

```
CREATE VIEW covidview AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://<storageaccountname>.dfs.core.windows.net/coviddata/covid_19_data.csv',
        FORMAT = 'CSV',
        PARSER_VERSION='2.0',
        DATA_SOURCE = 'SqlOnDemandDemo',
        FIRSTROW = 2
    ) WITH (
      SNo int,
      ObservationDate varchar(50),
      ProvinceState varchar(200),
      CountryRegion varchar(200),
      LastUpdate varchar(50),
      Confirmed decimal(18,2),
      Deaths decimal(18,2),
      Recovered decimal(18,2)
) AS [result]
```

- check the see if the data is loaded

```
select * from covidview
```

- Now lets create a table to store the data for persistence
- Have to create a master key in coviddb

```
use coviddb
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'xyxpojklnbgtyughd234!234$%';
```

- Now create a credential in coviddb
- to create the credential you need SAS token key.
- Go to ADLS gen2 container
- on the left menu click SAS token
- Select resources for which to provide access
- Select the time range 
- Click create sas token.
- Note: copy the SAS URL without the blob URI like below. This will only be available once so don't close the page until you validate the below steps.

```
-- create credentials for containers in our demo storage account
CREATE DATABASE SCOPED CREDENTIAL sqlondemand
WITH IDENTITY='SHARED ACCESS SIGNATURE',  
SECRET = '?sv=2019-12-12&ss=bfqt&srt=sco&sp=rwdlacupx&se=2021-10-08T20:03:10Z&st=2020-10-08T12:03:10Z&spr=https&sig=73FwbAOqT3VI6SQ%2FjX1E0CQDo0y7Sri8%2FdAdgOnGE8w%3D'
GO
CREATE EXTERNAL DATA SOURCE SqlOnDemandDemo WITH (
    LOCATION = 'https://<storageaccountname>.blob.core.windows.net',
    CREDENTIAL = sqlondemand
);
```

- we are going to drop the view since it already exist as above

```
DROP VIEW IF EXISTS covidview;
GO
```

- Let's create a new view with persisted storage

```
CREATE VIEW covidview AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'coviddata/covid_19_data.csv',
        FORMAT = 'CSV',
        PARSER_VERSION='2.0',
        DATA_SOURCE = 'SqlOnDemandDemo',
        FIRSTROW = 2
    ) WITH (
      SNo int,
      ObservationDate varchar(50),
      ProvinceState varchar(200),
      CountryRegion varchar(200),
      LastUpdate varchar(50),
      Confirmed decimal(18,2),
      Deaths decimal(18,2),
      Recovered decimal(18,2)
) AS [result]
```

- Let's validate, if data is loaded properly

```
select top 200 * from covidview;
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/coviddata1.jpg "ETL")

```
select count(*) from covidview;
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/coviddata2.jpg "ETL")

- Now lets do some SQL query to mimic ETL activities

```
Select CountryRegion, sum(Confirmed) as Confirmed, sum(Deaths) as Deaths, sum(Recovered) as Recovered
 from covidview 
group by CountryRegion
```

```
Select datepart(YEAR, ObservationDate) as year, datepart(MONTH, ObservationDate) as month, 
CountryRegion, 
sum(Confirmed) as Confirmed, sum(Deaths) as Deaths, sum(Recovered) as Recovered
 from covidview 
group by datepart(YEAR, ObservationDate), datepart(MONTH, ObservationDate),CountryRegion
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/coviddata3.jpg "ETL")

- Now we would like to store the output into another tabel for further processing or visualization
- To do that we need to create a data source
- Create a separate container in the portal as covidoutput
- the below syntax will not create new containers so if container doesn't exist will error

```
-- create a container called covidoutput in blob or adls container.
CREATE EXTERNAL DATA SOURCE mycovidioutput WITH (
    LOCATION = 'https://storageaccountname.blob.core.windows.net/covidoutput', CREDENTIAL = sqlondemand
);
GO
```

- now we file format for parquet, as more universal file format

```
CREATE EXTERNAL FILE FORMAT [ParquetFF] WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO
```

- Now time to store the ETL output

```
CREATE EXTERNAL TABLE [dbo].[covidaggrCETAS] WITH (
        LOCATION = 'covidAggr/',
        DATA_SOURCE = [mycovidioutput],
        FILE_FORMAT = [ParquetFF]
) AS
Select datepart(YEAR, ObservationDate) as year, datepart(MONTH, ObservationDate) as month, 
CountryRegion, 
sum(Confirmed) as Confirmed, sum(Deaths) as Deaths, sum(Recovered) as Recovered
 from covidview 
group by datepart(YEAR, ObservationDate), datepart(MONTH, ObservationDate),CountryRegion;
```

- Let validate and see if the data was saved (persisted) for further processing

```
USE [coviddb];
GO

SELECT
    *
FROM covidaggrCETAS;
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/coviddata4.jpg "ETL")

More to come