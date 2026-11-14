# PIPELINE
# Kafka Topic1 > Spark Word Count > Kafka Topic2

# NOTE: this requires running the program w/ spark-submit

import json
from kafka import KafkaConsumer, KafkaProducer
from kafka.serializer import Deserializer
from pyspark.sql import SparkSession
from datetime import datetime

class UTF8Deserializer(Deserializer):
    def __call__(self, value):
        return value.decode("utf-8")

consumer = KafkaConsumer(
    "topic1",
    bootstrap_servers = "localhost:9092",
    auto_offset_reset = "earliest",
    enable_auto_commit = True,
    value_deserializer = UTF8Deserializer()
)

producer = KafkaProducer(
    bootstrap_servers = "localhost:9092",
    value_serializer = lambda v: v.encode("utf-8")
)

spark = SparkSession.builder.appName("KafkaWordCount").getOrCreate()

# seconds after which the word counts compile
COUNT_INTERVAL = 10

try:
    start_time = datetime.now()
    data = []
    while True:
        for message in consumer:
            word = message.value
            print(f"Received: {word}")
            data.append(word)
        if (datetime.now() - start_time).total_seconds() >= COUNT_INTERVAL:
            rdd = spark.sparkContext.parallelize(data)
            word_counts = rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect()
            producer.send("topic2", value = json.dumps(word_counts))
            print(f"Sent: {json.dumps(word_counts)}")
            producer.flush()
            start_time = datetime.now()
            data = []
except KeyboardInterrupt:
    print("\nStopping consumer, producer, and Spark.")
finally:
    consumer.close()
    producer.close()
    spark.stop()
