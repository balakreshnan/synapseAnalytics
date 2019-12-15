--https://docs.microsoft.com/en-us/sql/t-sql/statements/copy-into-transact-sql?view=azure-sqldw-latest#examples
DROP table Consumption_Info
GO

create table Consumption_Info
(
    year char(4),
    month char(4),
    consumption DECIMAL(9,2)
)
GO

COPY INTO Consumption_Info
FROM 'https://taxidata.blob.core.windows.net/test-blob-container/consumptioninfo.csv'
WITH (
    FILE_TYPE = 'CSV',
    FIRSTROW = 2,
    CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='?st=2019-12-12T21%3A08%3A37Z&se=2025-12-31T18%3A00%3A00Z&sp=racwdl&sv=2018-03-28&sr=c&sig=OOCQdGDehYTif32x7Eqo0ljsU%2BXmzMStUIcfztniO2w%3D')
);

SELECT avg(consumption) from Consumption_Info
