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

mostPopular = connections.sort(func.col("connections").desc()).first()

mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()
# capAmericaID = (
#     names.filter(func.col("name") == "CAPTAIN AMERICA").select("id").first()[0]
# )

print(
    str(mostPopularName[0])
    + " is the most popular superhero with "
    + str(mostPopular[1])
    + " co-appearances."
)
