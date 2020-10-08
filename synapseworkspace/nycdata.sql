-- type your sql script here, we now have intellisense
Create database nyctaxi;

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK     'https://azureopendatastorage.blob.core.windows.net/nyctlc/yellow/puYear=*/puMonth=*/*.parquet',
        FORMAT = 'parquet'
    ) AS [result];

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK     'https://azureopendatastorage.blob.core.windows.net/nyctlc/green/puYear=*/puMonth=*/*.parquet',
        FORMAT = 'parquet'
    ) AS [result];

use nyctaxi;
GO

use nyctaxi
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'xyxpojklnbgtyughd234!234$%';
-- create credentials for containers in our demo storage account
use nyctaxi
CREATE DATABASE SCOPED CREDENTIAL sqlondemand
WITH IDENTITY='SHARED ACCESS SIGNATURE',  
SECRET = '?sv=2019-12-12&ss=bfqt&srt=sco&sp=rwdlacupx&se=2021-10-08T20:03:10Z&st=2020-10-08T12:03:10Z&spr=https&sig=73FwbAOqT3VI6SQ%2FjX1E0CQDo0y7Sri8%2FdAdgOnGE8w%3D'
GO

use nyctaxi
CREATE EXTERNAL DATA SOURCE SqlOnDemandDemo1 WITH (
    LOCATION = 'https://accsynapsestorage.blob.core.windows.net',
    CREDENTIAL = sqlondemand
);

use nyctaxi
-- create a container called covidoutput in blob or adls container.
CREATE EXTERNAL DATA SOURCE mycovidioutputyellow WITH (
    LOCATION = 'https://accsynapsestorage.blob.core.windows.net/nyctaxiyellow', CREDENTIAL = sqlondemand
);
GO

CREATE EXTERNAL DATA SOURCE mycovidioutputgreen WITH (
    LOCATION = 'https://accsynapsestorage.blob.core.windows.net/nyctaxigreen', CREDENTIAL = sqlondemand
);
GO

use nyctaxi
CREATE EXTERNAL FILE FORMAT [ParquetFF] WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO

use nyctaxi
CREATE EXTERNAL TABLE [dbo].[nycyellow] WITH (
        LOCATION = 'nyctaxiyellow/',
        DATA_SOURCE = [mycovidioutputyellow],
        FILE_FORMAT = [ParquetFF]
) AS
SELECT
    *
FROM
    OPENROWSET(
        BULK     'https://azureopendatastorage.blob.core.windows.net/nyctlc/yellow/puYear=*/puMonth=*/*.parquet',
        FORMAT = 'parquet'
    ) AS [result];


use nyctaxi
CREATE EXTERNAL TABLE [dbo].[nycgreen] WITH (
        LOCATION = 'nyctaxigreen/',
        DATA_SOURCE = [mycovidioutputgreen],
        FILE_FORMAT = [ParquetFF]
) AS
SELECT
    *
FROM
    OPENROWSET(
        BULK     'https://azureopendatastorage.blob.core.windows.net/nyctlc/green/puYear=*/puMonth=*/*.parquet',
        FORMAT = 'parquet'
    ) AS [result];



use nyctaxi;
GO



SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK     'https://accsynapsestorage.blob.core.windows.net/synapseroot/nyctaxiyellow/*',
        FORMAT = 'parquet'
    ) AS [result];

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK     'https://accsynapsestorage.blob.core.windows.net/synapseroot/nyctaxigreen/*',
        FORMAT = 'parquet'
    ) AS [result];


SELECT
    count(*)
FROM
    OPENROWSET(
        BULK     'https://accsynapsestorage.blob.core.windows.net/synapseroot/nyctaxiyellow/*',
        FORMAT = 'parquet'
    ) AS [result];

SELECT
    count(*)
FROM
    OPENROWSET(
        BULK     'https://accsynapsestorage.blob.core.windows.net/synapseroot/nyctaxigreen/*',
        FORMAT = 'parquet'
    ) AS [result];

SELECT
    Top 2000 *
FROM
    OPENROWSET(
        BULK     'https://accsynapsestorage.blob.core.windows.net/synapseroot/nyctaxiyellow/*',
        FORMAT = 'parquet'
    ) AS [result];

SELECT
    PuYear, PuMonth, sum(FareAmount) as FareAmount, sum(TotalAmount) as TotalAmount
FROM
    OPENROWSET(
        BULK     'https://accsynapsestorage.blob.core.windows.net/synapseroot/nyctaxiyellow/*',
        FORMAT = 'parquet'
    ) AS [result] 
    Group by PuYear, PuMonth
    Order by PuYear, PuMonth;

