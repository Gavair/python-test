from datetime import datetime
from elasticsearch import Elasticsearch


class ESClient:
    def __init__(self, host_name="elasticsearch", port=9200, size=100):
        self.size = size
        self.es = Elasticsearch({"host": host_name, "port": port, "scheme": "http"})

    def get_all_data(self):
        self.es.indices.refresh(index="main-index")
        resp = self.es.search(index="main-index", size=self.size, query={"match_all": {}})

        result = []
        for hit in resp["hits"]["hits"]:
            record = hit["_source"].copy()
            record["timestamp"] = str(datetime.strptime(record["timestamp"], "%Y-%m-%dT%H:%M:%S"))
            result += [record]

        return result
