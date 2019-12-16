-- type your sql script here, we now have intellisense
-- babal - testing.


DROP  VIEW VW_WeatherInfo
GO

CREATE VIEW VW_WeatherInfo
AS
SELECT
    *
FROM  
    OPENROWSET(
        BULK 'https://internalsandboxwe.dfs.core.windows.net/feliped/weather/*/*/*.parquet',
        FORMAT='PARQUET'
    ) AS nyc
GO

select count(*) from VW_WeatherInfo;

DROP VIEW VW_YTData
GO
CREATE VIEW VW_YTData
AS
SELECT
 *
FROM
 OPENROWSET(
BULK'https://internalsandboxwe.dfs.core.windows.net/feliped/nyctlc/yellow/*/*/*.parquet',
 FORMAT='PARQUET'
 ) AS nyc
GO

DROP VIEW VW_GTData
GO
CREATE VIEW VW_GTData
AS
SELECT
 *
FROM
 OPENROWSET(
BULK'https://internalsandboxwe.dfs.core.windows.net/feliped/nyctlc/green/*/*/*.parquet',
 FORMAT='PARQUET'
 ) AS nyc
GO

select count(*) from VW_YTData;
select count(*) from VW_GTData;

select count(YT.*) from VW_YTData YT inner join VW_WeatherInfo WE
ON YT.startlat = WE.Latitude 
WHERE YT.startlat is not null

select top 1000 * from VW_YTData;

select top 1000 * from VW_GTData;

select 
datepart(year,TPEPPICKUPDATETIME) as Year,
datepart(month,TPEPPICKUPDATETIME) as Month,
datepart(day,TPEPPICKUPDATETIME) as Day,
count(*) as Tcount
from VW_YTData 
group by datepart(year,TPEPPICKUPDATETIME),datepart(month,TPEPPICKUPDATETIME),datepart(day,TPEPPICKUPDATETIME)
order by datepart(year,TPEPPICKUPDATETIME),datepart(month,TPEPPICKUPDATETIME),datepart(day,TPEPPICKUPDATETIME)

select 
datepart(year,LPEPPICKUPDATETIME) as Year,
datepart(month,LPEPPICKUPDATETIME) as Month,
datepart(day,LPEPPICKUPDATETIME) as Day,
count(*) as Tcount
from VW_GTData 
group by datepart(year,LPEPPICKUPDATETIME),datepart(month,LPEPPICKUPDATETIME),datepart(day,LPEPPICKUPDATETIME)
order by datepart(year,LPEPPICKUPDATETIME),datepart(month,LPEPPICKUPDATETIME),datepart(day,LPEPPICKUPDATETIME)


select top 100 * from VW_WeatherInfo;

select 
datepart(year,DATETIME) as Year,
datepart(month,DATETIME) as Month,
datepart(day,DATETIME) as Day,
count(*) as Tcount
from VW_WeatherInfo 
group by datepart(year,DATETIME),datepart(month,DATETIME),datepart(day,DATETIME)
order by datepart(year,DATETIME),datepart(month,DATETIME),datepart(day,DATETIME)
