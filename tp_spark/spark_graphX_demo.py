
from pyspark.sql import SparkSession
from graphframes import GraphFrame

# List of Maven coordinates for all required packages
maven_packages = [
    "graphframes:graphframes:0.8.4-spark3.5-s_2.12",
    "org.scala-lang:scala-library:2.12",
    "com.singlestore:singlestore-jdbc-client:1.2.4",
    "com.singlestore:singlestore-spark-connector_2.12:4.1.8-spark-3.5.0",
    "org.apache.commons:commons-dbcp2:2.12.0",
    "org.apache.commons:commons-pool2:2.12.0",
    "io.spray:spray-json_3:1.3.6"
]

# Create Spark session with all required packages
spark = (SparkSession
             .builder
             .config("spark.jars.packages", ",".join(maven_packages))
             .appName("Spark GraphFrames Test")
             .getOrCreate()
        )

spark.sparkContext.setLogLevel("ERROR")

# Create a Vertex DataFrame with unique ID column "id"
v = spark.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
], ["id", "name", "age"])

# Create an Edge DataFrame with "src" and "dst" columns
e = spark.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
], ["src", "dst", "relationship"])

# Create a GraphFrame
from graphframes import *
g = GraphFrame(v, e)

# Query: Get in-degree of each vertex.
g.inDegrees.show()

# Query: Count the number of "follow" connections in the graph.
g.edges.filter("relationship = 'follow'").count()

# Run PageRank algorithm, and show results.
results = g.pageRank(resetProbability=0.01, maxIter=20)
results.vertices.select("id", "pagerank").show()