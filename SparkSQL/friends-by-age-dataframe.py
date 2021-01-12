from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

people = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("fakefriends-header.csv")
)

print("Here is our inferred schema:")
people.printSchema()

print("Group by age")
friendsByAge = people.select("age", "friends")
friendsByAge.groupBy("age").agg(
    func.round(func.avg("friends"), 2).alias("friends_avg")
).sort("age").show()

spark.stop()
