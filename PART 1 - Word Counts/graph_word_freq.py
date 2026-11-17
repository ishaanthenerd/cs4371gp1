# PIPELINE
# Kafka Topic2 > Kibana > Generate Graphs

import os
import json
from kafka import KafkaConsumer
from kafka.serializer import Deserializer
from elasticsearch import Elasticsearch, helpers
from kibana import Kibana

class UTF8Deserializer(Deserializer):
    def deserialize(self, topic, headers = None, data = None):
        if data is None:
            data = headers
        return data.decode("utf-8") if data is not None else None

consumer = KafkaConsumer(
    "topic2",
    bootstrap_servers = "localhost:9092",
    auto_offset_reset = "earliest",
    enable_auto_commit = True,
    value_deserializer = UTF8Deserializer()
)

ES_PASSWORD = os.getenv('ES_PASSWORD')
es = Elasticsearch(
    "https://localhost:9200",
    ca_certs="/etc/elasticsearch/certs/http_ca.crt",
    basic_auth=("elastic", ES_PASSWORD)
)
client = Kibana("http://localhost:5601")
INDEX = "converted-data"
MAX_WORDS = 10

def convert_dict(word_counts, index = INDEX):
    if not es.indices.exists(index = index):
        es.indices.create(
            index = index,
            mappings = {
                "properties": {
                    "category": {"type": "keyword"},
                    "value": {"type": "integer"}
                }
            }
        )
    documents = [
        {
            "_index": index,
            "_id": category,
            "_source": {
                "category": category,
                "value": value
            }
        }
        for category, value in word_counts.items()
    ]
    helpers.bulk(es, documents)
    es.indices.refresh(index = index)
    print(f"Indexed {len(documents)} documents into {index}")

# takes in dict: str -> int, outputs the bar plot using Kibana
def create_kibana_bar_plot(index = INDEX):
    chart = {
        "type": "xy",
        "title": "Values by Category",
        "query": {"expression": "", "language": "kql"},
        "layers": [
            {
                "type": "bar_stacked",

                "data_source": {
                    "type": "data_view_spec",
                    "index_pattern": index
                },

                "x": {
                    "operation": "terms",
                    "fields": ["category"],
                    "limit": 10
                },

                "y": [
                    {
                        "operation": "sum",
                        "field": "value",
                        "label": "Total Value"
                    }
                ]
            }
        ]
    }

    result = client.visualizations.create(data = chart)
    print("Visualization created, ID =", result.body["id"])

chart_created = False

try:
    while True:
        for message in consumer:
            j = message.value
            print(f"Received: {j}")
            word_counts = json.loads(j)
            word_counts = dict(
                sorted(
                    word_counts.items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:MAX_WORDS]
            )
            convert_dict(word_counts)
            if not chart_created:
                create_kibana_bar_plot()
                chart_created = True
except KeyboardInterrupt:
    print("\nStopping consumer and Kibana.")
finally:
    consumer.close()
    client.close()
