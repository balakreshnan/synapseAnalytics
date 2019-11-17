# synapseAnalytics

## Get started with Synapse Analytics
First create Azure SQL Pool and Spark cluster.

Azure SQL DW Pool is used for running SQL queries. Query tool uses Azure SQL DW pools to execute queries.

Spark cluster is used for Notebooks. Notebooks can run Pyspark, Scala or Spark SQL. Also C# is also available.

## SQl Scripts
Create a new sql scripts and run DML and DDL on the query. Select the data base as dwpool (database name created by SQL DW pool).
Create master key for polybase.
Create table to load data.
Run Select commands to view the data.

## Notebooks
Select scala as runtime engine
Select the spark cluster to run the code
Create code using cells
Run cells to evecute.

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/notebook0.JPG "Notebook Spark Version")


I was able to run a linear regression model with taxidata to predict tips.

## Data Flow
Select source as Blob storage to load the source data. in the optimize change the data type needed.
Use Select transformation to select the columns.
Use Sink as Azure SQL DW to save the data into sql DW pools.
Use Blob storage as staging to load the data into Azure SQL DW pools.

## Orchestrate - Pipelines
Create pipeline to run Data flow to load the data from blob and insert into Azure SQL DW Pools

Create pipeline to run the Linear regression notebook using spark.

I was able to run parallely both.

## Monitor
Check the monitor page for status.
