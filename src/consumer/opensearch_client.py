from opensearchpy import OpenSearch, RequestsHttpConnection
from config import OPENSEARCH_CONFIG

class OpenSearchClient:
    def __init__(self):
        self.client = OpenSearch(
            hosts=[{'host': OPENSEARCH_CONFIG['host'], 'port': OPENSEARCH_CONFIG['port']}],
            # http_auth=(OPENSEARCH_CONFIG['username'], OPENSEARCH_CONFIG['password']),
            use_ssl=OPENSEARCH_CONFIG['use_ssl'],
            verify_certs=OPENSEARCH_CONFIG['verify_certs'],
            # connection_class=RequestsHttpConnection,
            ssl_assert_hostname = False,
            ssl_show_warn = False
        )

    def index_record(self, index, record, record_id=None):
        response = self.client.index(
            index=index,
            body=record,
            id=record_id
        )
        return response