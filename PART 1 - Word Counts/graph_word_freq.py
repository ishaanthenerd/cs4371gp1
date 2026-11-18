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
    ca_certs="/home/ishaanshiveshhome1/certs/http_ca.crt",
    basic_auth=("elastic", ES_PASSWORD)
)
# debug
print("Elasticsearch connected:", es.info())

client = Kibana(
    "http://localhost:5601",
    basic_auth=("elastic", ES_PASSWORD)
)
INDEX = "converted-data"

def convert_dict(word_counts, index=INDEX):
    if not es.indices.exists(index=index):
        es.indices.create(
            index=index,
            mappings={
                "properties": {
                    "category": {"type": "keyword"},
                    "value": {"type": "integer"}
                }
            }
        )

    for category, value in word_counts.items():
        es.update(
            index=index,
            id=category,
            script={
                "source": """
                    if (ctx.op == 'create') {
                        ctx._source.category = params.category;
                        ctx._source.value = params.value;
                    } else {
                        ctx._source.value += params.value;
                    }
                """,
                "params": {
                    "category": category,
                    "value": value
                }
            },
            upsert={
                "category": category,
                "value": value
            }
        )

    es.indices.refresh(index=index)
    print(f"Updated {len(word_counts)} word counts in {index}")

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
            # Kibana filters for first 10 counts automatically
            # no need to filter word_counts
            convert_dict(word_counts)
            if not chart_created:
                create_kibana_bar_plot()
                chart_created = True
except KeyboardInterrupt:
    print("\nStopping consumer and Kibana.")
finally:
    consumer.close()
    client.close()
