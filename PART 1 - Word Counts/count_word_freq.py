# PIPELINE
# Kafka Topic1 > Spark Word Count > Kafka Topic2

# NOTE: this requires running the program w/ spark-submit

import json
from kafka import KafkaConsumer, KafkaProducer
from pyspark.sql import SparkSession
from datetime import datetime

consumer = KafkaConsumer(
    "topic1",
    bootstrap_servers = "localhost:9092",
    auto_offset_reset = "earliest",
    enable_auto_commit = True,
    value_deserializer = lambda v: v.decode("utf-8")
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
        # Wait at most 1 second for messages
        records = consumer.poll(timeout_ms=1000)

        for messages in records.values():
            for message in messages:
                word = message.value
                print(f"Received: {word}")
                data.append(word)

        # Check whether the counting interval has elapsed
        if (datetime.now() - start_time).total_seconds() >= COUNT_INTERVAL:

            if data:
                rdd = spark.sparkContext.parallelize(data)

                word_counts = (
                    rdd.map(lambda x: (x, 1))
                       .reduceByKey(lambda x, y: x + y)
                       .collect()
                )

                # Convert list of tuples into a dictionary
                word_counts = dict(word_counts)

                producer.send(
                    "topic2",
                    value=json.dumps(word_counts)
                )

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

'''
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
'''