import json
import logging
from datetime import datetime
from fastapi import FastAPI
from kafka import KafkaProducer

logging.getLogger().setLevel(logging.INFO)

app = FastAPI()
producer = KafkaProducer(bootstrap_servers=["broker:29092"], value_serializer=lambda x: json.dumps(x).encode("utf-8"))


@app.get("/insert/{text}")
def insert_text(text: str):

    if not 1 <= len(text) <= 20:
        raise Exception("Input message should be in range [1; 20]")

    if not text.isalnum() or not text.islower():
        raise Exception("Input message should contain only lowercase characters and digits")

    logging.info(f'Received "{text}"')

    producer.send("main", {"timestamp": int(datetime.now().timestamp()), "message": text})

    return {"text": text}
