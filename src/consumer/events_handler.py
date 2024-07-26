import json
import logging
from opensearch_client import OpenSearchClient


class EventsHandler:
    def __init__(self, msg) -> None:
        self.msg = msg
        self.opensearch_client = OpenSearchClient()

    def handle_message(self):
        """
        Handle Message Method.
        Indexes events into opensearch
        """
        try:
            record = json.loads(self.msg.value().decode("utf-8"))
            # Extract the 'after' part of the record
            if "after" in record and record["after"] is not None:
                data = record["after"]["value"]
                key = record["after"]["key"]
                response = self.opensearch_client.index_record(
                    index="cdc",  # index name is cdc
                    record=data,
                    record_id=key,
                )
                logging.info(
                    f"Message ingested into OpenSearch with ID: {response['_id']}"
                )
        except Exception as e:
            logging.error(f"Failed to process message: {str(e)}")
            raise e
