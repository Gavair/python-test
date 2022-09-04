import logging
from datetime import datetime
from fastapi import FastAPI
from elasticsearch import Elasticsearch

logging.getLogger().setLevel(logging.INFO)

app = FastAPI()


class ESClient:
    def __init__(self, host_name="elasticsearch", port=9200):
        self.es = Elasticsearch({"host": host_name, "port": port, "scheme": "http"})

    def get_all_data(self):
        self.es.indices.refresh(index="main-index")

        resp = self.es.search(index="main-index", query={"match_all": {}})

        result = []
        for hit in resp["hits"]["hits"]:
            record = hit["_source"].copy()
            record["timestamp"] = str(datetime.strptime(record["timestamp"], "%Y-%m-%dT%H:%M:%S"))
            result += [record]

        return result


es = ESClient()


@app.get("/show")
def read_item():

    result = es.get_all_data()
    logging.info(f" Got: {result}")

    return {"data": result}
