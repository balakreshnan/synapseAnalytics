-- This is auto-generated code
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://accsynapsestorage.dfs.core.windows.net/coviddata/covid_19_data.csv',
        FORMAT = 'CSV',
        PARSER_VERSION='2.0',
        FIRSTROW = 2
    ) AS [result]

CREATE DATABASE coviddb;

CREATE VIEW covidview AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://accsynapsestorage.dfs.core.windows.net/coviddata/covid_19_data.csv',
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

use coviddb
GO

use coviddb
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'xyxpojklnbgtyughd234!234$%';
-- create credentials for containers in our demo storage account
CREATE DATABASE SCOPED CREDENTIAL sqlondemand
WITH IDENTITY='SHARED ACCESS SIGNATURE',  
SECRET = '?sv=2019-12-12&ss=bfqt&srt=sco&sp=rwdlacupx&se=2021-10-08T20:03:10Z&st=2020-10-08T12:03:10Z&spr=https&sig=73FwbAOqT3VI6SQ%2FjX1E0CQDo0y7Sri8%2FdAdgOnGE8w%3D'
GO
CREATE EXTERNAL DATA SOURCE SqlOnDemandDemo WITH (
    LOCATION = 'https://accsynapsestorage.blob.core.windows.net',
    CREDENTIAL = sqlondemand
);


DROP VIEW IF EXISTS covidview;
GO

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

select top 200 * from covidview;

select count(*) from covidview;

Select CountryRegion, sum(Confirmed) as Confirmed, sum(Deaths) as Deaths, sum(Recovered) as Recovered
 from covidview 
group by CountryRegion

Select datepart(YEAR, ObservationDate) as year, datepart(MONTH, ObservationDate) as month, 
CountryRegion, 
sum(Confirmed) as Confirmed, sum(Deaths) as Deaths, sum(Recovered) as Recovered
 from covidview 
group by datepart(YEAR, ObservationDate), datepart(MONTH, ObservationDate),CountryRegion

-- create a container called covidoutput in blob or adls container.
CREATE EXTERNAL DATA SOURCE mycovidioutput WITH (
    LOCATION = 'https://accsynapsestorage.blob.core.windows.net/covidoutput', CREDENTIAL = sqlondemand
);
GO

CREATE EXTERNAL FILE FORMAT [ParquetFF] WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO

Drop external table covidaggrCETAS;

CREATE EXTERNAL TABLE [dbo].[covidaggrCETAS] WITH (
        LOCATION = 'covidParquet/',
        DATA_SOURCE = [mycovidioutput],
        FILE_FORMAT = [ParquetFF]
) AS
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
) AS [result];

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

USE [coviddb];
GO

SELECT
    *
FROM covidaggrCETAS;