# Adventure works SQL data base to Parquet

## Migrate Azure SQL database to parquet format for spark processing

## Create Azure SQL sample database

- Create a resource group called sqldemo
- Create a new resource as database
- Select Azure SQL database
- Create a database name as db1
- create a database server name as db1servername
- Create a user name as sqluser
- Creae a password
- Select a region of chose
- in networking select allow azure service to access database
- in networking select add client ip to firewall
- in advanced select sample database

## Use Azure data factory to move to ADLS gen2

- Idea here is connect to Azure SQL and copy all tables
- Create a Linked services as source for SQL we created above
- Create a Integrate
- Drag and drop Lookup
- Select the adventure works LT database

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/advworks1.jpg "Synapse Analytics")

- Select query 

```
select Table_schema, Table_name from information_schema.tables 
where table_type ='BASE TABLE' 
```

- Make sure first row is not selected
- Now drag and drop Foreach
- in the settings tab

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/advworks2.jpg "Synapse Analytics")

```
@activity('GetTables').output.value 
```

- Now click the activity and create a new
- Name the activity as ExportToParquet
- Select source as Adventure works SQL database
- Select sink as ADLS gen2 with parguet
- Select Use query

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/advworks3.jpg "Synapse Analytics")

```
@concat('select * from ',item().Table_Schema,'.',item().Table_Name) 
```

- Select sink 
- as a dataset properties

```
@concat(item().Table_Schema,'_',item().Table_Name)
```

- now in the data set property for ADLS gen2 parquet
- open and in Filepath

```
@{formatDateTime(utcnow(),'yyyy')}/@{formatDateTime(utcnow(),'MM')}/@{formatDateTime(utcnow(),'dd')}/@{concat(dataset().FileName,'_',formatDateTime(utcnow(),'yyyyMMdd'),'.parquet')} 
```

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/advworks4.jpg "Synapse Analytics")

- Make sure note the URL
- Commit all
- Create a pull request
- Merge the changes
- Then switch to Main branch
- Click publish
- Now open the integrate pipeline and click add trigger and select trigger now

## Setup spark database and tables using notebook

- Create a notebook
- in one cell change to c#
- Create database first

```
spark.Sql("CREATE DATABASE adventureworkslt")
```

- Lets create spark tables (managed tables)
- Make sure the name of the parquet file
- Create a customer table

```
%%pyspark
spark.sql("CREATE TABLE IF NOT EXISTS adventureworkslt.customer USING Parquet LOCATION 'abfss://containername@storagename.dfs.core.windows.net/adventureworkslt/2020/12/29/SalesLT_Customer_20201229.parquet '")
```

- Create address table

```
%%pyspark
spark.sql("CREATE TABLE IF NOT EXISTS adventureworkslt.address USING Parquet LOCATION 'abfss://containername@storagename.dfs.core.windows.net/adventureworkslt/2020/12/29/SalesLT_Address_20201229.parquet '")
```

- Create customeraddress

```
%%pyspark
spark.sql("CREATE TABLE IF NOT EXISTS adventureworkslt.customeraddress USING Parquet LOCATION 'abfss://containername@storagename.dfs.core.windows.net/adventureworkslt/2020/12/29/SalesLT_CustomerAddress_20201229.parquet '")
```

- Create product

```
%%pyspark
spark.sql("CREATE TABLE IF NOT EXISTS adventureworkslt.product USING Parquet LOCATION 'abfss://containername@storagename.dfs.core.windows.net/adventureworkslt/2020/12/29/SalesLT_Product_20201229.parquet '")
```

- Create product category

```
%%pyspark
spark.sql("CREATE TABLE IF NOT EXISTS adventureworkslt.productcategory USING Parquet LOCATION 'abfss://containername@storagename.dfs.core.windows.net/adventureworkslt/2020/12/29/SalesLT_ProductCategory_20201229.parquet '")
```

- Create product description

```
%%pyspark
spark.sql("CREATE TABLE IF NOT EXISTS adventureworkslt.productdescription USING Parquet LOCATION 'abfss://containername@storagename.dfs.core.windows.net/adventureworkslt/2020/12/29/SalesLT_ProductDescription_20201229.parquet '")
```

- Create product model

```
%%pyspark
spark.sql("CREATE TABLE IF NOT EXISTS adventureworkslt.productmodel USING Parquet LOCATION 'abfss://containername@storagename.dfs.core.windows.net/adventureworkslt/2020/12/29/SalesLT_ProductModel_20201229.parquet '")
```

- Create productmodelproductdescription

```
%%pyspark
spark.sql("CREATE TABLE IF NOT EXISTS adventureworkslt.productmodelproductdescription USING Parquet LOCATION 'abfss://containername@storagename.dfs.core.windows.net/adventureworkslt/2020/12/29/SalesLT_ProductModelProductDescription_20201229.parquet '")
```

- Create Sales order detail

```
%%pyspark
spark.sql("CREATE TABLE IF NOT EXISTS adventureworkslt.salesorderdetail USING Parquet LOCATION 'abfss://containername@storagename.dfs.core.windows.net/adventureworkslt/2020/12/29/SalesLT_SalesOrderDetail_20201229.parquet '")
```

- Create Sales order header

```
%%pyspark
spark.sql("CREATE TABLE IF NOT EXISTS adventureworkslt.salesorderheader USING Parquet LOCATION 'abfss://containername@storagename.dfs.core.windows.net/adventureworkslt/2020/12/29/SalesLT_SalesOrderHeader_20201229.parquet '")
```

- Now time to validate few tables to see if the data is there

```
%%pyspark
display(spark.sql("select * from adventureworkslt.customer"))
```

```
%%pyspark
display(spark.sql("select * from adventureworkslt.customeraddress"))
```

```
%%pyspark
display(spark.sql("select * from adventureworkslt.product"))
```

```
%%pyspark
display(spark.sql("select * from adventureworkslt.productmodel"))
```

```
%%pyspark
display(spark.sql("select * from adventureworkslt.salesorderdetail"))
```

```
%%pyspark
display(spark.sql("select * from adventureworkslt.salesorderheader"))
```

## Use notebook and load same data into dataframe for scala processing

- Create a notebook
- Select scala
- Select the spark pool
- load the data

```
val customerDF = spark.read.parquet("abfss://containername@storagename.dfs.core.windows.net/adventureworkslt/2020/12/29/SalesLT_Customer_20201229.parquet ")
val addressDF = spark.read.parquet("abfss://containername@storagename.dfs.core.windows.net/adventureworkslt/2020/12/29/SalesLT_Address_20201229.parquet ")
val customeraddressDF = spark.read.parquet("abfss://containername@storagename.dfs.core.windows.net/adventureworkslt/2020/12/29/SalesLT_CustomerAddress_20201229.parquet ")
val productDF = spark.read.parquet("abfss://containername@storagename.dfs.core.windows.net/adventureworkslt/2020/12/29/SalesLT_Product_20201229.parquet ")
val productcategoryDF = spark.read.parquet("abfss://containername@storagename.dfs.core.windows.net/adventureworkslt/2020/12/29/SalesLT_ProductCategory_20201229.parquet ")
val productdescriptionDF = spark.read.parquet("abfss://containername@storagename.dfs.core.windows.net/adventureworkslt/2020/12/29/SalesLT_ProductDescription_20201229.parquet ")
val productmodelDF = spark.read.parquet("abfss://containername@storagename.dfs.core.windows.net/adventureworkslt/2020/12/29/SalesLT_ProductModel_20201229.parquet ")
val productmodeldescriptionDF = spark.read.parquet("abfss://containername@storagename.dfs.core.windows.net/adventureworkslt/2020/12/29/SalesLT_ProductModelProductDescription_20201229.parquet ")

val salesorderdetailDF = spark.read.parquet("abfss://containername@storagename.dfs.core.windows.net/adventureworkslt/2020/12/29/SalesLT_SalesOrderDetail_20201229.parquet ")
val salesorderheaderDF = spark.read.parquet("abfss://containername@storagename.dfs.core.windows.net/adventureworkslt/2020/12/29/SalesLT_SalesOrderHeader_20201229.parquet ")
```

- display and validate if dataframe is loaded

```
display(customerDF)
```