from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs


def loadMoviesNames():
    movieNames = {}
    with codecs.open(
        "ml-100k/u.item", "r", encoding="ISO-8859-1", errors="ignore"
    ) as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
    return movieNames


spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Broadcast - Spark Context, the old RDD interface
# This is going to broadcast out to every executor on my cluster
# nameDict is the broadcasted object, not the dictionary itself,
# We need to retrieve it from the object later on.
nameDict = spark.sparkContext.broadcast(loadMoviesNames())

schema = StructType(
    [
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

moviesDF = spark.read.option("sep", "\t").schema(schema).csv("ml-100k/u.data")

movieCounts = moviesDF.groupBy("movieID").count()

# Create a user-defined function to Look up movie names from our broadcasted dictionary
def lookupName(movieID):
    # .value on the broadcasted object to get the dictionary back
    return nameDict.value[movieID]


lookupNameUDF = func.udf(lookupName)

# Add a movieTitle column using our new udf
moviesWithNames = movieCounts.withColumn(
    "movieTitle", lookupNameUDF(func.col("movieID"))
)

# Sort the results
sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

sortedMoviesWithNames.show(10, False)

spark.stop()
