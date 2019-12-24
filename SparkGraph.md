# Azure synapse analytics spark processing graph

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/synapseprocess.JPG "Synapse Analytics")

## Load data

```
spark.conf.set(
  "fs.azure.account.key.waginput.blob.core.windows.net",
  "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
```

## Load Station Data

```
val dfstation = spark.read.option("header","true").option("inferSchema","true").csv("wasbs://graphdata@waginput.blob.core.windows.net/station.csv")
display(dfstation)
```

## load Trip Data

```
val dftrip = spark.read.option("header","true").option("inferSchema","true").csv("wasbs://graphdata@waginput.blob.core.windows.net/trip.csv")
display(dftrip)
```

## Display schema for review

```
dfstation.printSchema()
dftrip.printSchema()
```

## Create graph based data

### Build the Graph

Now that you've imported your data, you're going to need to build your graph. To do so you're going to do two things. You are going to build the structure of the vertices (or nodes) and you're going to build the structure of the edges. What's awesome about GraphFrames is that this process is incredibly simple. All that you need to do get the distinct id values in the Vertices table and rename the start and end stations to src and dst respectively for your edges tables. These are required conventions for vertices and edges in GraphFrames.

```
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
```

```
val justStations = dfstation.selectExpr("float(id) as station_id", "name").distinct()
```

```
val stations = dftrip.select("start_station_id").withColumnRenamed("start_station_id", "station_id").union(dftrip.select("end_station_id").withColumnRenamed("end_station_id", "station_id")).distinct().select(col("station_id").cast("long").alias("value"))
```

```
val stationVertices: RDD[(VertexId, String)] = stations.join(justStations, stations("value") === justStations("station_id")).select(col("station_id").cast("long"), col("name")).rdd.map(row => (row.getLong(0), row.getString(1))) // maintain type information
```

```
val stationEdges:RDD[Edge[Long]] = dftrip.select(col("start_station_id").cast("long"), col("end_station_id").cast("long")).rdd.map(row => Edge(row.getLong(0), row.getLong(1), 1))
```

Now you can build your graph. 

You're also going to cache the input DataFrames to your graph.

```
val defaultStation = ("Missing Station") 
val stationGraph = Graph(stationVertices, stationEdges, defaultStation)
stationGraph.cache()
```

```
println("Total Number of Stations: " + stationGraph.numVertices)
println("Total Number of Trips: " + stationGraph.numEdges)
// sanity check
println("Total Number of Trips in Original Data: " + dftrip.count)
```

### Trips From Station to Station

One question you might ask is what are the most common destinations in the dataset from location to location. You can do this by performing a grouping operator and adding the edge counts together. This will yield a new graph except each edge will now be the sum of all of the semantically same edges. Think about it this way: you have a number of trips that are the exact same from station A to station B, you just want to count those up!

In the below query you'll see that you're going to grab the station to station trips that are most common and print out the top 10.


```
val ranks = stationGraph.pageRank(0.0001).vertices
ranks.join(stationVertices).sortBy(_._2._1, ascending=false).take(10).foreach(x => println(x._2._2))
```

```
stationGraph.groupEdges((edge1, edge2) => edge1 + edge2).triplets.sortBy(_.attr, ascending=false).map(triplet => "There were " + triplet.attr.toString + " trips from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").take(10).foreach(println)
```
You can see above that a given vertex being a Caltrain station seems to be significant! This makes sense as these are natural connectors and likely one of the most popular uses of these bike share programs to get you from A to B in a way that you don't need a car!


### In Degrees and Out Degrees

Remember that in this instance you've got a directed graph. That means that your trips are directional - from one location to another. Therefore you get access to a wealth of analysis that you can use. You can find the number of trips that go into a specific station and leave from a specific station.

Naturally you can sort this information and find the stations with lots of inbound and outbound trips! Check out this definition of Vertex Degrees for more information.

Now that you've defined that process, go ahead and find the stations that have lots of inbound and outbound traffic.

```
stationGraph.inDegrees.join(stationVertices).sortBy(_._2._1, ascending=false).take(10).foreach(x => println(x._2._2 + " has " + x._2._1 + " in degrees."))
```

```
stationGraph.outDegrees.join(stationVertices).sortBy(_._2._1, ascending=false).take(10).foreach(x => println(x._2._2 + " has " + x._2._1 + " out degrees."))
```

One interesting follow up question you could ask is what is the station with the highest ratio of in degrees but fewest out degrees. As in, what station acts as almost a pure trip sink. A station where trips end at but rarely start from

```
stationGraph.inDegrees.join(stationGraph.outDegrees).join(stationVertices).map(x => (x._2._1._1.toDouble/x._2._1._2.toDouble, x._2._2)).sortBy(_._1, ascending=false).take(5).foreach(x => println(x._2 + " has a in/out degree ratio of " + x._1))
```

You can do something similar by getting the stations with the lowest in degrees to out degrees ratios, meaning that trips start from that station but don't end there as often. This is essentially the opposite of what you have above.


```
stationGraph.inDegrees.join(stationGraph.inDegrees).join(stationVertices).map(x => (x._2._1._1.toDouble/x._2._1._2.toDouble, x._2._2)).sortBy(_._1).take(5).foreach(x => println(x._2 + " has a in/out degree ratio of " + x._1))
```

The conclusions of what you get from the above analysis should be relatively straightforward. If you have a higher value, that means many more trips come into that station than out, and a lower value means that many more trips leave from that station than come into it!

Hopefully you've gotten some value out of this notebook! Graph stuctures are everywhere once you start looking for them and hopefully GraphFrames will make analyzing them easy!
