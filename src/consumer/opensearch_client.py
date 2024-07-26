import threading
import logging
from typing import Any, Dict, Optional
from opensearchpy import OpenSearch, exceptions as opensearch_exceptions
from config import OPENSEARCH_CONFIG


class ThreadSafeSingleton(type):
    # Dictionary to store instances of the singleton classes
    _instances = {}
    # Lock to ensure thread-safe instance creation
    _lock: threading.Lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        """
        Ensures only one instance of the class is created.
        """
        if cls not in cls._instances:
            with cls._lock:
                if cls not in cls._instances:
                    instance = super().__call__(*args, **kwargs)
                    cls._instances[cls] = instance
        return cls._instances[cls]


class OpenSearchClient(metaclass=ThreadSafeSingleton):
    def __init__(self):
        """
        Initialize Opensearch Client
        """
        self.client = OpenSearch(
            hosts=[
                {
                    "host": OPENSEARCH_CONFIG["host"],
                    "port": OPENSEARCH_CONFIG["port"],
                }
            ],
            use_ssl=OPENSEARCH_CONFIG["use_ssl"],
            verify_certs=OPENSEARCH_CONFIG["verify_certs"],
            ssl_assert_hostname=False,
            ssl_show_warn=False,
        )

    def index_record(
        self,
        index: str,
        record: Dict[str, Any],
        record_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Indexes a record into the specified OpenSearch index.
        :param: index: The name of the OpenSearch index where the record will be stored.
        :param: record: The record data to be indexed, represented as a dictionary.
        :param: record_id:  An optional unique identifier for the record. If not provided, OpenSearch will generate one. 
        :return: The response from the OpenSearch server after indexing the record.

        """
        try:
            response = self.client.index(
                index=index, body=record, id=record_id
            )
            return response
        except opensearch_exceptions.ConnectionError as e:
            logging.error(
                f"Connection error while indexing record {record_id}: {e}"
            )
            raise
        except Exception as e:
            logging.error(
                f"An unexpected error occurred while indexing record {record_id}: {e}"
            )
            raise
