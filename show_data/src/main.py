import logging
from fastapi import FastAPI

from es_client import ESClient

logging.getLogger().setLevel(logging.INFO)

app = FastAPI()
es = ESClient()


@app.get("/show")
def read_item():

    result = es.get_all_data()
    logging.info(f" Got: {result}")

    return {"data": result}
