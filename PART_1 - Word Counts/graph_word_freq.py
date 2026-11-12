# PIPELINE
# Kafka Topic2 > Kibana > Generate Graphs

import json
from kafka import KafkaConsumer
from kibana import Kibana

consumer = KafkaConsumer(
    "topic2",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: v.decode("utf-8")
)

client = Kibana("http://localhost:5601") # TODO: check if this should be 9092

# takes in dict: str -> int, outputs the bar plot using Kibana
def create_kibana_bar_plot(word_counts):
    # TODO
    pass

MAX_WORDS = 10

try:
    while True:
        for message in consumer:
            j = message.value
            word_counts = json.loads(j)
            word_counts = sorted(word_counts, key=lambda x: x[1], reverse=True)[:MAX_WORDS]
            create_kibana_bar_plot(word_counts)
except KeyboardInterrupt:
    print("\nStopping consumer and Kibana.")
finally:
    consumer.close()
    client.close()