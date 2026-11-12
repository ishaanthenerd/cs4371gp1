# PIPELINE
# Financial News > Extract Headlines > Kafka Topic1

import finnhub
from kafka import KafkaProducer

# BANNED - read from environment (TODO)
API_KEY = "dbq8tt1r01qn88eeid10dbq8tt1r01qn88eeid1g"
finnhub_client = finnhub.Client(api_key=API_KEY)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: v.encode("utf-8")
)

def extract_headline():
    news_selection = rd.choice(['general', 'crypto', 'merger']) # forex removed b/c only one news article appears in it
    responses = finnhub_client.general_news(news_selection, min_id=0)
    random_response = rd.choice(responses)
    return random_response.get("headline")

# seconds between consecutive headline extractions
EXTRACT_INTERVAL = 0.1

try:
    while True:
        sleep(EXTRACT_INTERVAL)
        headline = extract_headline()
        words = headline.split()
        for word in words:
            producer.send("topic1", value=word)
            producer.flush()
            print(f"Sent: {word}")
except KeyboardInterrupt:
    print("\nStopping producer.")
finally:
    producer.close()