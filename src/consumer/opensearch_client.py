import threading
import logging
from typing import Any, Dict, Optional
from opensearchpy import OpenSearch, exceptions as opensearch_exceptions
from config import OPENSEARCH_CONFIG


class ThreadSafeSingleton(type):
    _instances = {}
    _lock: threading.Lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            with cls._lock:
                if cls not in cls._instances:
                    instance = super().__call__(*args, **kwargs)
                    cls._instances[cls] = instance
        return cls._instances[cls]


class OpenSearchClient(metaclass=ThreadSafeSingleton):
    def __init__(self):
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
