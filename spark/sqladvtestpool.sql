-- type your sql script here, we now have intellisense

SELECT TOP 100 * 
FROM 
OPENROWSET(BULK N'https://wagssynap.dfs.core.windows.net/output/mlsample1.csv', FORMAT = 'csv') as r


CREATE DATABASE SCOPED CREDENTIAL ConsumptionBlob
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = '?st=2019-12-12T21%3A08%3A37Z&se=2025-12-31T18%3A00%3A00Z&sp=racwdl&sv=2018-03-28&sr=c&sig=OOCQdGDehYTif32x7Eqo0ljsU%2BXmzMStUIcfztniO2w%3D';
GO

CREATE EXTERNAL DATA SOURCE ConsumptionData_CSV
    WITH (
        TYPE = BLOB_STORAGE,
        LOCATION = 'https://taxidata.blob.core.windows.net',
        CREDENTIAL = ConsumptionBlob
    );
GO

SELECT * FROM OPENROWSET(BULK 'test-blob-container/consumptioninfo.csv',DATA_SOURCE = 'ConsumptionData_CSV',FORMAT = 'CSV') AS DataFile;
GO


    COPY INTO Consumption_Info
FROM 'https://taxidata.blob.core.windows.net/test-blob-container/consumptioninfo.csv'
WITH (
    FILE_TYPE = 'CSV',
    FIRSTROW = 2,
    CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='?st=2019-12-12T21%3A08%3A37Z&se=2025-12-31T18%3A00%3A00Z&sp=racwdl&sv=2018-03-28&sr=c&sig=OOCQdGDehYTif32x7Eqo0ljsU%2BXmzMStUIcfztniO2w%3D')
);

SELECT avg(consumption) from Consumption_Info
