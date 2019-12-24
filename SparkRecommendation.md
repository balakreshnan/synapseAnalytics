# Azure Synapse Analytics Run Recommendation model

Run Recommendation supervised learning with Movie data set. 

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/synapseprocess.JPG "Synapse Analytics")

## Syanpse Advanced Analytics

Synapse has the ability to run spark based code which leads to Data engineering or feature engineering and also Machine learning. This articles describes how to train a machine learning model using spark in synapse.

## Prerequiste

- Prepare Data set
Download the training data set. Training dataset is newyork city taxi data set which is avaialble for public.
here is my copy available in case:
https://github.com/balakreshnan/synapseAnalytics/tree/master/datasets/movies

- Move/copy data into blob storage or ADLS gen2

Upload the above training file to blob storage or ADLS Gen2. Or you can use synapse orchestrate feature to move the data into Blob.

```
For my testing i was able to move the blob storage train.csv into ADLS gen2 filesystem. I did that for just to show how to move data inside synapse analytics.
```
