import sys
import os
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1
if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingRecieverKafkaWordCount")
    ssc = StreamingContext(sc, 50) # 2 second window
    #broker, topic = sys.argv[1:]
    broker = 'localhost:9092'
    topic = 'new_topic'
    kvs = KafkaUtils.createStream(ssc,'localhost:2181', "raw-event",{'new_topic':1})
    lines = kvs.map(lambda x: x[1])
    lines.pprint()
    wlist = lines.flatMap(lambda line: line.split(" "))
    wlist.pprint()
    #counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
    #counts.pprint()
    ssc.start()
    ssc.awaitTermination()
