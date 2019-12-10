# Synapse Analytics Tensorflow Hello world

Run tensorflow code in Azure synapse analytics. Sample code to test if tensorflow is working. 

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/synapseprocess.JPG "Synapse Analytics")

## Syanpse Advanced Analytics

Synapse has the ability to run spark based code which leads to Data engineering or feature engineering and also Machine learning. This articles describes how to train a machine learning model using spark in synapse.

## Use case

Try to import minst data set and build machine learning model using tensorflow. Idea here is to test and see if tensorflow is working and every one apply their own use cases and play with tensorflow in Azure synapse Analytics.

## Prerequiste

Use Scala notebook.

Check if tensorflow is installed. To do that print the version
```
import tensorflow as tf; 
print(tf.__version__)
```

the time this document was created the output was: 1.14.0. The version can change as new development new spark version are released.

## Tensorflow code sample.

Let's import necessary imports.

```
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse

# Import data
from tensorflow.examples.tutorials.mnist import input_data

import tensorflow as tf
```

## Load dataset

Load the minst data set available inside the envrionment. Minst is already loaded by the above import.

```
mnist = input_data.read_data_sets('/tmp/data', one_hot=True)
```

## Assign varibles

Set placeholder and assign variables with default values for tensforflow. Here is where we are also declaring variables to be used by the tensorflow modelling.

```
x = tf.placeholder(tf.float32, [None, 784])
W = tf.Variable(tf.zeros([784, 10]))
b = tf.Variable(tf.zeros([10]))
y = tf.matmul(x, W) + b 
```

```
y_ = tf.placeholder(tf.float32, [None, 10])
```

## configure softmax to converge

In mathematics, the softmax function, also known as softargmax[1] or normalized exponential function,[2]:198 is a function that takes as input a vector of K real numbers, and normalizes it into a probability distribution consisting of K probabilities proportional to the exponentials of the input numbers. That is, prior to applying softmax, some vector components could be negative, or greater than one; and might not sum to 1; but after applying softmax, each component will be in the interval 
( 0 , 1 ) {\displaystyle (0,1)} , and the components will add up to 1, so that they can be interpreted as probabilities

```
tf.reduce_mean(-tf.reduce_sum(y_ * tf.log(tf.nn.softmax(y)), reduction_indices=[1]))
```

## configure cross entrophy and Gradient descent

In information theory, the cross entropy between two probability distributions 
p and q over the same underlying set of events measures the average number of bits needed to identify an event drawn from the set if a coding scheme used for the set is optimized for an estimated probability distribution 
q , rather than the true distribution p. 

Gradient descent is a first-order iterative optimization algorithm for finding the minimum of a function. To find a local minimum of a function using gradient descent, one takes steps proportional to the negative of the gradient (or approximate gradient) of the function at the current point. If, instead, one takes steps proportional to the positive of the gradient, one approaches a local maximum of that function; the procedure is then known as gradient ascent. Gradient descent was originally proposed by Cauchy in 1847.

```
cross_entropy = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(logits=y, labels=y_))
train_step = tf.train.GradientDescentOptimizer(0.5).minimize(cross_entropy)
```

## configure model parameters

```
correct_prediction = tf.equal(tf.argmax(y, 1), tf.argmax(y_, 1))
accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))
summary = tf.summary.scalar("accuracy", accuracy)
```

## invoke tensorflow session and run model

Here is where tensorflow session is invoked and each batch is run to build the model.

```
sess = tf.InteractiveSession()

# Make sure to use the same log directory for both start TensorBoard in your training.
summary_writer = tf.summary.FileWriter(log_dir, graph=sess.graph)

tf.global_variables_initializer().run()
for batch in range(1000):
  batch_xs, batch_ys = mnist.train.next_batch(100)
  _, batch_summary = sess.run([train_step, summary], feed_dict={x: batch_xs, y_: batch_ys})
  summary_writer.add_summary(batch_summary, batch)

```

## Check the output of model

```
print(sess.run(accuracy, feed_dict={x: mnist.test.images,
                                    y_: mnist.test.labels}))
```
