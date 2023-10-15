from locust import HttpUser, task, events, constant_throughput
from logging import getLogger, StreamHandler, INFO
import pathlib

endpoint = ""
queries = {}

logger = getLogger(__name__)
logger.setLevel(INFO)
ch = StreamHandler()
ch.setLevel(INFO)
logger.addHandler(ch)


def create_graphql_query(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        query = {
            "oprationName": file_path.stem,
            "variables": "{}",
            "query": f.read()
        }
    return query


for path in pathlib.Path('/mnt/locust/').glob('*.graphql'):
    queries[path.stem] = create_graphql_query(path)


class GraphQLClient(HttpUser):
    wait_time = constant_throughput(1)

    def on_start(self):
        logger.info("Start Pre-Warm")

    @task
    def pre_warm(self):
        for query in queries.keys():
            self.client.post(
                endpoint,
                headers={'Content-Type': 'application/json'},
                data=queries[query],
                name=query
            )

    @events.request.add_listener
    def on_request(request_type, name, response_time, response_length, exception, **kwargs):
        if exception:
            logger.error(f'"Result": "Failure", "QueryName": {name}, "Exception": {exception}')

    def on_stop(self):
        logger.info("End Pre-Warm")
