from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("AmountSpentByCustomer")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(",")
    customer_id = int(fields[0])
    amount_spent = float(fields[2])
    return (customer_id, amount_spent)


lines = sc.textFile("customer-orders.csv")
rdd = lines.map(parseLine)
sumAmountByCustomer = rdd.reduceByKey(lambda x, y: round(x + y, 1))
sortedAmount = sumAmountByCustomer.map(lambda x: (x[1], x[0])).sortByKey()
results = sortedAmount.collect()


for result in results:
    print(result)
