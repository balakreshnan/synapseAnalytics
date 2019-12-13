# Azure synapse spark vs Azure Data Bricks

Hi All, Need some help. I created a large instance instance with 3 to 10 node max. loaded a 55 million row taxi data form blob into spark dataframe - in batch mode and then did a df.count() and then removed duplicates usign dropDuplicates and then do a count again. 
I did that both in Azure synapse Spark and also Azure databricks.

Azure synapse spark:
Compute spec: Large 16vCPu/128Gb ram

spark read: Command executed in 2mins 22s 57ms by babal on 12-13-2019 08:51:14.388 -06:00

First df.count(): Command executed in 1mins 32s 516ms by babal on 12-13-2019 08:54:25.826 -06:00

dedup

Second df.count(): Command executed in 1mins 23s 606ms by babal on 12-13-2019 08:56:52.743 -06:00

Total row count for Both: res13: Long = 55423856

Azure databricks:

Compute spec: 4 cores, 14GB Ram

spark.read took: Command took3.19 minutes-- by babal@microsoft.com at 12/13/2019, 9:00:09 AM

First df.count(): Command took1.18 minutes-- by babal@microsoft.com at 12/13/2019, 9:10:32 AM

dedup

Second df.count(): Command took12.80 seconds-- by babal@microsoft.com at 12/13/2019, 9:14:55 AM

Total row count for Both: res13: Long = 55423856

Is this by design or any suggestions please let me know.

