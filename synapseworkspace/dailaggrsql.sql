-- type your sql script here, we now have intellisense

Drop Table [wwi].[dailyaggr];

CREATE TABLE [wwi].[dailyaggr]
( 
	[tsYear] [bigint]  NOT NULL,
	[tsMonth] [bigint]  NOT NULL,
	[tsDay] [bigint]  NULL,
	[tsHour] [bigint]  NULL,
	[avgFare] [real]  NULL
)
WITH
(
	DISTRIBUTION = HASH ( [tsYear] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO


Select  * from [wwi].[dailyaggr];

Select  count(*) from [wwi].[dailyaggr];