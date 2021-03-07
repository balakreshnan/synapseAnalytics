# Azure Purview Search inside Azure Synapse Analytics

## Search Catalog and build data pipeline in Azure synapse studio

## Use Case

- Ability to find assets
- Ability to create ETL/ELT or data flow data processing
- Catalog integrated data processing

## Pre requistie

- Azure account
- Azure Synapse analytics account
- Azure purview 
- Configure scan for synapse analytics studio
- Provide contributor access to purview managed identity to synapse studio
- Connect Synapse studio to Purview

## Steps

- First of connect to synapse studio to azure purview
- https://docs.microsoft.com/en-us/azure/synapse-analytics/catalog-and-governance/quickstart-connect-azure-purview#:~:text=%20Connect%20an%20Azure%20Purview%20Account%20%201,in%20the%20tab%20Azure%20Purview%20account.%20More
- Log into Synapse studio
- Go to Data
- on the top search bar search for your asset

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/purview1.jpg "Synapse Analytics")

- Now you can also see the schema and lineage ( if available)
- Classification is also shown

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/purview2.jpg "Synapse Analytics")

- From here we can create a integrate data processing pipeline or linked data set

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/purview3.jpg "Synapse Analytics")

- Now we can create a data flow
- Data flow allows us to do Transformation to data

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/purview4.jpg "Synapse Analytics")

- It's amazing to search what is available in data lake and then check the schema and create ETL/ELT or data engineering from one UI