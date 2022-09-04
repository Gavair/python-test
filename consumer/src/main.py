import json
import logging
from datetime import datetime
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

logging.getLogger().setLevel(logging.INFO)

es = Elasticsearch({"host": "elasticsearch", "port": 9200, "scheme": "http"})

consumer = KafkaConsumer(
    "main",
    bootstrap_servers=["broker:29092"],
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)


def main():
    for message in consumer:
        message: dict = message.value

        message["timestamp"] = datetime.fromtimestamp(message["timestamp"])
        logging.info(f"Got {message}")

        response = es.index(index="main-index", document=message)
        logging.info(f"ElasticSearch response: {response}")


if __name__ == "__main__":
    main()
