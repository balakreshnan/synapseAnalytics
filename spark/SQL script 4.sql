SELECT
    TOP 100 *
FROM  
    OPENROWSET(
        BULK 'https://wagssynap.dfs.core.windows.net/poutput/poutput/part-00000-a69c74fa-84b6-4a3e-93dc-2a220f369033-c000.snappy.parquet',
        FORMAT='PARQUET'
    ) AS nyc;
