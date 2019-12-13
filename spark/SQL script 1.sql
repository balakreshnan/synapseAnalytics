-- type your sql script here, we now have intellisense

select top 200 * from dbo.taxidata;

CREATE MASTER KEY ENCRYPTION BY PASSWORD = '23987hxJ#KL95234nl0zBe';
GO

OPEN MASTER KEY DECRYPTION BY PASSWORD = '23987hxJ#KL95234nl0zBe' 

--DROP MASTER KEY

select count(*) from taxidata;

truncate table dbo.taxidata;

drop table dbo.taxidata;

create table taxidata
(
id bigint identity(1,1) not null,
fare_amount float,
pickup_datetime datetime,
pickup_longitude varchar(250),
pickup_latitude varchar(250),
dropoff_longitude varchar(250),
dropoff_latitude varchar(250),
passenger_count int
)

select top 10 * from taxidata;

select top 10 * from taxidata1;

select count(*) from taxidata1;

CREATE USER [prgandhi@microsoft.com] FROM EXTERNAL PROVIDER;
GO

ALTER ROLE dbmanager ADD MEMBER [prgandhi@microsoft.com];

ALTER ROLE db_owner ADD MEMBER [prgandhi@microsoft.com];

EXEC sp_addrolemember 'dbmanager', 'prgandhi@microsoft.com';

GRANT SELECT ON DATABASE::dwpool TO [prgandhi@microsoft.com];

GRANT INSERT ON DATABASE::dwpool TO [prgandhi@microsoft.com];
GRANT UPDATE ON DATABASE::dwpool TO [prgandhi@microsoft.com];
GRANT DELETE ON DATABASE::dwpool TO [prgandhi@microsoft.com];
GRANT EXECUTE ON DATABASE::dwpool TO [prgandhi@microsoft.com];
GRANT CONTROL ON DATABASE::dwpool TO [prgandhi@microsoft.com];
GRANT ALTER ON DATABASE::dwpool TO [prgandhi@microsoft.com];


CREATE USER [anlo@microsoft.com] FROM EXTERNAL PROVIDER;
GO

ALTER ROLE dbmanager ADD MEMBER [anlo@microsoft.com];

ALTER ROLE db_owner ADD MEMBER [anlo@microsoft.com];

EXEC sp_addrolemember 'dbmanager', 'anlo@microsoft.com';

GRANT SELECT ON DATABASE::dwpool TO [anlo@microsoft.com];

GRANT INSERT ON DATABASE::dwpool TO [anlo@microsoft.com];
GRANT UPDATE ON DATABASE::dwpool TO [anlo@microsoft.com];
GRANT DELETE ON DATABASE::dwpool TO [anlo@microsoft.com];
GRANT EXECUTE ON DATABASE::dwpool TO [anlo@microsoft.com];
GRANT CONTROL ON DATABASE::dwpool TO [anlo@microsoft.com];
GRANT ALTER ON DATABASE::dwpool TO [anlo@microsoft.com];



CREATE DATABASE SCOPED CREDENTIAL [MyCredential]
    WITH
      IDENTITY = 'babal@microsoft.com',
      SECRET = 'FsnZrS+XU+CObmL7yT2SvdLo4UCzR7JvgdW+HLPJVKLIOu+bnHBdBMaa09sHI6eUXeEPVhk3eGFCYd25chG+3g=='


CREATE EXTERNAL DATA SOURCE [MyDataSource]
    WITH (
      TYPE = HADOOP,
      LOCATION = 'abfss://output@wagssynap.dfs.core.windows.net',
      CREDENTIAL = MyCredential
    );

CREATE EXTERNAL FILE FORMAT [MyFileFormat]
  WITH (  
    FORMAT_TYPE = PARQUET,  
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'  
 ); 

CREATE EXTERNAL FILE FORMAT textdelimited1  
WITH (  
    FORMAT_TYPE = DELIMITEDTEXT,  
    FORMAT_OPTIONS (  
        FIELD_TERMINATOR = ','),  
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.DefaultCodec'
);




