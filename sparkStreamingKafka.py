# -*- coding: utf-8 -*-
"""

@author: Hugo Fajardo
This program connects to Apache Kafka and reads the content of a registered topic.
Processes theme content in Apache Spark with Structured Streaming.

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lower
from pyspark.sql.functions import split
from pyspark.ml.feature import Normalizer, StandardScaler

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

servidor_bootstrap = '192.168.0.2:9092'
topic = 'Twitter'

spark = SparkSession \
    .builder \
    .master("local[8]") \
    .appName("TwitterWordCount") \
    .config("spark.sql.streaming.metricsEnabled", True) \
    .config("spark.sql.shuffle.partitions", 5) \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

lines = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", servidor_bootstrap) \
  .option("subscribe", topic) \
  .option("startingOffsets", "latest") \
  .load()

lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Split the lines into words
words = lines.select(explode(split(lines.value, " ")).alias("word"))

# Generate running word count
wordCounts = words.groupBy("word").count().orderBy('count', ascending=False)

# Start running the query that prints the running counts to the console
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
