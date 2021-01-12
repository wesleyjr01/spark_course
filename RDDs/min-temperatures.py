from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)

lines = sc.textFile("1800.csv")


def parseLine(line):
    fields = line.split(",")
    name = fields[0]
    type_ = fields[2]
    temperature = round(float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0, 2)
    return (name, type_, temperature)


rdd = lines.map(parseLine)
filtered_rdd = rdd.filter(lambda x: "TMIN" in x[1])
keyValues = filtered_rdd.map(lambda x: (x[0], x[2]))
minValues = keyValues.reduceByKey(lambda x, y: min(x, y))
results = minValues.collect()

for result in results:
    print(f"{result[0]} : {result[1]}F")
