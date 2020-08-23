#spark streaming API can consume and process live data streams
#below are few use case
# Streaming ETL (Event-driven ETL)--> data is continually cleaned and aggregated before it is pushed into data stores
# Trigger event detection
# Spark Streaming allows security analysts to check against known threats prior to passing the packets on to the storage platform

from pyspark import SparkConf,SparkContext
import time

conf=SparkConf()
conf.set("spark.executor.memory", "1g")
conf.set("spark.cores.max", "2")
conf.setAppName("Abhishek-SparkStreaming")

sc = SparkContext('local[2]', conf=conf) # spark context with two threads

from pyspark.streaming import StreamingContext

streamContext=StreamingContext(sc,3)

totalLines=0
lines=streamContext.socketTextStream("localhost",9001)
print(type(lines))
#count lines
totalLines=0
linesCount=0
#word count within RDD

words=lines.flatMap(lambda line:line.split(" "))
pairs=words.map(lambda word: (word,1))
wordsCount=pairs.reduceByKey(lambda x,y:x+y)
wordsCount.pprint(2)

def computeMetrics(rdd):
    global totalLines
    global linesCount
    linesCount=rdd.count()
    totalLines+=linesCount
    print(rdd.collect())
    print("Lines in RDD:", linesCount," Total lines ",totalLines)

lines.foreachRDD(computeMetrics)

def windowMetrics(rdd):
    print("windows RDD size: ",rdd.count())

windowedRDD=lines.window(6,3)
windowedRDD.foreachRDD(windowMetrics)

streamContext.start()
print(streamContext)
print("starting")
time.sleep(40)
streamContext.stop()

time.sleep(42)