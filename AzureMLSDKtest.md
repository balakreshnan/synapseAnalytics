# Azure Synapse Analytics - Automated Machine learning using Azure Machine learning service.

Unified Analytics Tool to ingest, compute or process data, Store data, Advanced analytics or machine learning and Display all in one tool. End to end data analytics platform built to scale and ease of use. 

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/synapseprocess.JPG "Synapse Analytics")

## Syanpse Advanced Analytics

Synapse has the ability to run spark based code which leads to Data engineering or feature engineering and also Machine learning. This articles describes how to train a machine learning model using spark in synapse.

## Prerequiste
- Create Azure Machine learning services. Select the region as same as synapse analytics.
- Create a pyspark notebook.
- Run each batch statement in separate cell in notebook.

## Check version to see if library is available.
```
import azureml.core
print(azureml.core.VERSION)
```

## Setup the Azure machine learning workspace configuration
```
import os

subscription_id = "xxxxxxxxx-xxxxxxxxxxx-xxxxxxxxxx-xxxxxxx"
resource_group = "eastus_xxxxx_rg"
workspace_name = "eastus_xxxxx_ws"
workspace_region = "East US 2"
```

## Load the worspace details and write to config file

Workspace doesn't exist the below code will also create the workspace

```
from azureml.core import Workspace

try:
    ws = Workspace(subscription_id = subscription_id, resource_group = resource_group, workspace_name = workspace_name)
    # write the details of the workspace to a configuration file to the notebook library
    ws.write_config()
    print("Workspace configuration succeeded. Skip the workspace creation steps below")
except:
    print("Workspace not accessible. Change your parameters or create a new workspace below")
```

## Load the config file

Load the workspace information from the config file. usually the config file is a JSON file.

```
import azureml.core
from azureml.core import Workspace, Datastore

ws = Workspace.from_config()
```

## setup compute for Azure Automated ML

Build a cpu or gpu cluster. Depending on how much processing is needed create a cpu or gpu cluster. Below code creates
cpu cluster for the tutorial. Cluster already exist, then use that as well.

```
from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.core.compute_target import ComputeTargetException

# Choose a name for your CPU cluster
cpu_cluster_name = "cpucluster"

# Verify that cluster does not exist already
try:
    cpu_cluster = ComputeTarget(workspace=ws, name=cpu_cluster_name)
    print("Found existing cpucluster")
except ComputeTargetException:
    print("Creating new cpucluster")
    
    # Specify the configuration for the new cluster
    compute_config = AmlCompute.provisioning_configuration(vm_size="STANDARD_D2_V2",
                                                           min_nodes=0,
                                                           max_nodes=4)

    # Create the cluster with the specified name and configuration
    cpu_cluster = ComputeTarget.create(ws, cpu_cluster_name, compute_config)
    
    # Wait for the cluster to complete, show the output log
    cpu_cluster.wait_for_completion(show_output=True)
```

## Now use data prep library

Python data preparation sdk. Here is the link for details.
https://docs.microsoft.com/en-us/python/api/azureml-dataprep/?view=azure-ml-py

```
import azureml.dataprep as dprep
```

## Load the dataset

Load sample data set for feature engineering using data prep library. This dataset is used for our automated machine learning.

```
dataset_root = "https://dprepdata.blob.core.windows.net/demo"

green_path = "/".join([dataset_root, "green-small/*"])
yellow_path = "/".join([dataset_root, "yellow-small/*"])

green_df = dprep.read_csv(path=green_path, header=dprep.PromoteHeadersMode.GROUPED)
# auto_read_file will automatically identify and parse the file type, and is useful if you don't know the file type
yellow_df = dprep.auto_read_file(path=yellow_path)

green_df.head(5)
yellow_df.head(5)
```

## Set the coulmns to use

Drop columns if they are null values. Get the columns that can be used for Machine learning model.

```
all_columns = dprep.ColumnSelector(term=".*", use_regex=True)
drop_if_all_null = [all_columns, dprep.ColumnRelationship(dprep.ColumnRelationship.ALL)]
useful_columns = [
    "cost", "distance", "dropoff_datetime", "dropoff_latitude", "dropoff_longitude",
    "passengers", "pickup_datetime", "pickup_latitude", "pickup_longitude", "store_forward", "vendor"
]
```

## create a tmp dataframe

Replace NA, drop nulls and change columns name to be common between datasets. Combine both datasets to form one compelete one.

New Cell:
```
tmp_df = (green_df
    .replace_na(columns=all_columns)
    .drop_nulls(*drop_if_all_null)
    .rename_columns(column_pairs={
        "VendorID": "vendor",
        "lpep_pickup_datetime": "pickup_datetime",
        "Lpep_dropoff_datetime": "dropoff_datetime",
        "lpep_dropoff_datetime": "dropoff_datetime",
        "Store_and_fwd_flag": "store_forward",
        "store_and_fwd_flag": "store_forward",
        "Pickup_longitude": "pickup_longitude",
        "Pickup_latitude": "pickup_latitude",
        "Dropoff_longitude": "dropoff_longitude",
        "Dropoff_latitude": "dropoff_latitude",
        "Passenger_count": "passengers",
        "Fare_amount": "cost",
        "Trip_distance": "distance"
     })
    .keep_columns(columns=useful_columns))
tmp_df.head(5)
```

New Cell:
```
green_df = tmp_df
```

New Cell:
```
tmp_df = (yellow_df
    .replace_na(columns=all_columns)
    .drop_nulls(*drop_if_all_null)
    .rename_columns(column_pairs={
        "vendor_name": "vendor",
        "VendorID": "vendor",
        "vendor_id": "vendor",
        "Trip_Pickup_DateTime": "pickup_datetime",
        "tpep_pickup_datetime": "pickup_datetime",
        "Trip_Dropoff_DateTime": "dropoff_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "store_and_forward": "store_forward",
        "store_and_fwd_flag": "store_forward",
        "Start_Lon": "pickup_longitude",
        "Start_Lat": "pickup_latitude",
        "End_Lon": "dropoff_longitude",
        "End_Lat": "dropoff_latitude",
        "Passenger_Count": "passengers",
        "passenger_count": "passengers",
        "Fare_Amt": "cost",
        "fare_amount": "cost",
        "Trip_Distance": "distance",
        "trip_distance": "distance"
    })
    .keep_columns(columns=useful_columns))
tmp_df.head(5)
```

New Cell:
```
yellow_df = tmp_df
combined_df = green_df.append_rows([yellow_df])
```

## Profile the dataset

Requests the data profile which collects summary statistics on the full data produced by the Dataflow. A data profile can be very useful to understand the input data, identify anomalies and missing values, and verify that data preparation operations produced the desired result.
https://docs.microsoft.com/en-us/python/api/azureml-dataprep/azureml.dataprep.dataflow?view=azure-ml-py


```
decimal_type = dprep.TypeConverter(data_type=dprep.FieldType.DECIMAL)
combined_df = combined_df.set_column_types(type_conversions={
    "pickup_longitude": decimal_type,
    "pickup_latitude": decimal_type,
    "dropoff_longitude": decimal_type,
    "dropoff_latitude": decimal_type
})
combined_df.keep_columns(columns=[
    "pickup_longitude", "pickup_latitude", 
    "dropoff_longitude", "dropoff_latitude"
]).get_profile()
```

```
tmp_df = (combined_df
    .drop_nulls(
        columns=["pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude"],
        column_relationship=dprep.ColumnRelationship(dprep.ColumnRelationship.ANY)
    ) 
    .filter(dprep.f_and(
        dprep.col("pickup_longitude") <= -73.72,
        dprep.col("pickup_longitude") >= -74.09,
        dprep.col("pickup_latitude") <= 40.88,
        dprep.col("pickup_latitude") >= 40.53,
        dprep.col("dropoff_longitude") <= -73.72,
        dprep.col("dropoff_longitude") >= -74.09,
        dprep.col("dropoff_latitude") <= 40.88,
        dprep.col("dropoff_latitude") >= 40.53
    )))
tmp_df.keep_columns(columns=[
    "pickup_longitude", "pickup_latitude", 
    "dropoff_longitude", "dropoff_latitude"
]).get_profile()
```

## combine the dataframes
Reassign the tmp_df to combined_df

```
combined_df = tmp_df
```

Profile the combined df.
```
combined_df.keep_columns(columns='store_forward').get_profile()
```

## Fill, replace the dataset with missing values
Replace 0 with N for Store_forward columns. Also fill nulls as N.

```
combined_df = combined_df.replace(columns="store_forward", find="0", replace_with="N").fill_nulls("store_forward", "N")
```

Replace .00 with 0 for distance columns. Also fill nulls as 0.

```
combined_df = combined_df.replace(columns="distance", find=".00", replace_with=0).fill_nulls("distance", 0)
combined_df = combined_df.to_number(["distance"])
```

Rename columns and combine and profile.
```
tmp_df_renamed = (tmp_df
    .rename_columns(column_pairs={
        "pickup_datetime_1": "pickup_date",
        "pickup_datetime_2": "pickup_time",
        "dropoff_datetime_1": "dropoff_date",
        "dropoff_datetime_2": "dropoff_time"
    }))
tmp_df_renamed.head(5)
```

```
combined_df = tmp_df_renamed
combined_df.get_profile()
```

## drop columns not needed
```
tmp_df = tmp_df.drop_columns(columns=["pickup_datetime", "dropoff_datetime"])
```

Performs a pull on the data and populates conversion_candidates with automatically inferred conversion candidates for each column.
https://docs.microsoft.com/en-us/python/api/azureml-dataprep/azureml.dataprep.api.builders.columntypesbuilder?view=azure-ml-py

```
type_infer = tmp_df.builders.set_column_types()
type_infer.learn()
type_infer
```

Uses current state of this object to add 'set_column_types' step to the original Dataflow
https://docs.microsoft.com/en-us/python/api/azureml-dataprep/azureml.dataprep.api.builders.columntypesbuilder?view=azure-ml-py

```
tmp_df = type_infer.to_dataflow()
tmp_df.get_profile()
```

## filter columns

Take only values greater than 0.

```
tmp_df = tmp_df.filter(dprep.col("distance") > 0)
tmp_df = tmp_df.filter(dprep.col("cost") > 0)
```

```
import azureml.dataprep as dprep
```

# Build Automated Machine Learning code

At this point the data is ready. Now we are going to configure the automated machine learning with specification or parameters. Submit then to automated machine learning to run.

## import for Building automated Machine learning specification to submit.

Load the necessary import for automated machine learning.

```
import azureml.core
import pandas as pd
from azureml.core.workspace import Workspace
import logging
```

## Load workspaces

Load the workspace configuration to submit for modelling.

```
ws = Workspace.from_config()
# choose a name for the run history container in the workspace
experiment_name = 'automated-ml-regression'
# project folder
project_folder = './automated-ml-regression'

import os

output = {}
output['SDK version'] = azureml.core.VERSION
output['Subscription ID'] = ws.subscription_id
output['Workspace'] = ws.name
output['Resource Group'] = ws.resource_group
output['Location'] = ws.location
output['Project Directory'] = project_folder
pd.set_option('display.max_colwidth', -1)
outputDf = pd.DataFrame(data = output, index = [''])
outputDf.T
```

## Set columns

- Set the columns for features
- Set the label column

```
dflow_X = dflow_prepared.keep_columns(['pickup_weekday','pickup_hour', 'distance','passengers', 'vendor'])
dflow_y = dflow_prepared.keep_columns('cost')
```

## Split data set

Split the data set for training and testing. Training is used for training the model. Test is used for validating. Split is 80% training and 20% testing. Values are selected randomly. Flatten the data frames to make it available for model.

```
from sklearn.model_selection import train_test_split


x_df = dflow_X.to_pandas_dataframe()
y_df = dflow_y.to_pandas_dataframe()

x_train, x_test, y_train, y_test = train_test_split(x_df, y_df, test_size=0.2, random_state=223)
# flatten y_train to 1d array
y_train.values.flatten()
```

## Automated ML settings

Configure the automated machine learning parameters or specification. Set the iteration, timeout, primary metric to calculate, logging info, and cross validation values.

```
automl_settings = {
    "iteration_timeout_minutes" : 10,
    "iterations" : 30,
    "primary_metric" : 'spearman_correlation',
    "preprocess" : True,
    "verbosity" : logging.INFO,
    "n_cross_validations": 5
}
```

## set the automl config to model run

Set the above configuration to automated machine learning. Select the model as regression, provide the path, training values for fetures and label and also the above automated machine learning parameters/specficiation to run.

```
from azureml.train.automl import AutoMLConfig

# local compute 
automated_ml_config = AutoMLConfig(task = 'regression',
                             debug_log = 'automated_ml_errors.log',
                             path = project_folder,
                             X = x_train.values,
                             y = y_train.values.flatten(),
                             **automl_settings)
```

## Run the model

Submit the model to run. This step will take. Mine took close to 12 mins to run. i used cpu cluster with 4 nodes max.

```
from azureml.core.experiment import Experiment
experiment=Experiment(ws, experiment_name)
local_run = experiment.submit(automated_ml_config, show_output=True)
```

## Metrics calculation

Calculate the metric from the iterations and average them.

```
children = list(local_run.get_children())
metricslist = {}
for run in children:
    properties = run.get_properties()
    metrics = {k: v for k, v in run.get_metrics().items() if isinstance(v, float)}
    metricslist[int(properties['iteration'])] = metrics

rundata = pd.DataFrame(metricslist).sort_index(1)
rundata
```

## Print best model

Display the best model. Get the model's output metric to show the accuracy of the model.

```
best_run, fitted_model = local_run.get_output()
print(best_run)
print(fitted_model)
```

Register the model so we can use the id to deploy as web service to inference.

```
description = 'Automated Machine Learning Model'
tags = None
local_run.register_model(description=description, tags=tags)
print(local_run.model_id) # Use this id to deploy the model as a web service in Azure
```

## Print the predicted output

Display predicted output and validate the output to make sure if the model is performing well.

```
y_predict = fitted_model.predict(x_test.values) 
print(y_predict[:10])
```

To make the model better work on feature engineering and try to re run the models and see which model performs well.
