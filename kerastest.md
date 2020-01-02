# Synapse Analytics Keras Hello world

Run tensorflow 1.14 keras code in Azure synapse analytics. Sample code to test if tensorflow is working. 

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/synapseprocess.JPG "Synapse Analytics")

## Syanpse Advanced Analytics

Synapse has the ability to run spark based code which leads to Data engineering or feature engineering and also Machine learning. This articles describes how to train a machine learning model using spark in synapse.

## Use case

to run keras hellow world model in azure synapse analytics using tensorflow 1.14

## Prerequiste

Use Python notebook.

Check if tensorflow is installed. To do that print the version
```
import tensorflow as tf
import numpy as np
from tensorflow import keras
```

```
print(tf.__version__)
```

```
import matplotlib.pyplot as plt
```

the time this document was created the output was: 1.14.0. The version can change as new development new spark version are released.

## Keras code sample.

## setup the model

```
model = tf.keras.Sequential([keras.layers.Dense(units=1, input_shape=[1])])

model.compile(optimizer='sgd', loss='mean_squared_error')
```

## define sample data set for training and testing

```
xs = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], dtype=float)
ys = np.array([1.0, 1.5, 2.0, 2.5, 3.0, 3.5], dtype=float)
```

## run model

```
model.fit(xs, ys, epochs=1000)
print(model.predict([7.0]) * 100)
```

## plot the output

```
plt.title("Regression graph: house size relative to price")
plt.xlabel('Number of bedrooms')
plt.ylabel('House prices')
plt.plot(xs, ys)
plt.show()
```

If want to try more samples search for keras tensorflow 1.14 examples in search engine.