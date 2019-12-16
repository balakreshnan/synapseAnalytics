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

```
val ranks = stationGraph.pageRank(0.0001).vertices
ranks.join(stationVertices).sortBy(_._2._1, ascending=false).take(10).foreach(x => println(x._2._2))
```

```
stationGraph.groupEdges((edge1, edge2) => edge1 + edge2).triplets.sortBy(_.attr, ascending=false).map(triplet => "There were " + triplet.attr.toString + " trips from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").take(10).foreach(println)
```

```
stationGraph.inDegrees.join(stationVertices).sortBy(_._2._1, ascending=false).take(10).foreach(x => println(x._2._2 + " has " + x._2._1 + " in degrees."))
```

```
stationGraph.outDegrees.join(stationVertices).sortBy(_._2._1, ascending=false).take(10).foreach(x => println(x._2._2 + " has " + x._2._1 + " out degrees."))
```

```
stationGraph.inDegrees.join(stationGraph.outDegrees).join(stationVertices).map(x => (x._2._1._1.toDouble/x._2._1._2.toDouble, x._2._2)).sortBy(_._1, ascending=false).take(5).foreach(x => println(x._2 + " has a in/out degree ratio of " + x._1))
```

```
stationGraph.inDegrees.join(stationGraph.inDegrees).join(stationVertices).map(x => (x._2._1._1.toDouble/x._2._1._2.toDouble, x._2._2)).sortBy(_._1).take(5).foreach(x => println(x._2 + " has a in/out degree ratio of " + x._1))
```
