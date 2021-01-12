from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType(
    [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
)

names = spark.read.schema(schema).option("sep", " ").csv("Marvel_Names")

# Load each line of the file and throw into a single column called 'value'
lines = spark.read.text("Marvel_Graph")

connections = (
    lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0])
    .withColumn(
        "connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1
    )
    .groupBy("id")
    .agg(func.sum("connections").alias("connections"))
)

minConnectionCount = connections.agg(func.min("connections")).first()[0]

minConnections = connections.filter(func.col("connections") == minConnectionCount)

minConnectionsWithNames = minConnections.join(names, "id")

print(f"The following characters have only {str(minConnectionCount)} connections")

minConnectionsWithNames.select("name").show()

spark.close()