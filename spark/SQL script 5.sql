SELECT
    TOP 100 *
FROM  
    OPENROWSET(
        BULK 'https://wagssynap.dfs.core.windows.net/telemetry/raw/telemetry/part-00000-dc4957e4-2feb-4804-a625-b80450cb7bb9-c000.snappy.parquet',
        FORMAT='PARQUET'
    ) AS nyc;
