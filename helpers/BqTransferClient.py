from google.cloud import bigquery_datatransfer

from helpers.BqClient import *
from helpers.StaticMethods import *

class BqTransferClient(BqClient):
    """Helper class (child of BqClient) designed to help with deployment to BQ."""
    def __init__(self, client_name):
        BqClient.__init__(self, client_name, True)
        self.instance = bigquery_datatransfer.DataTransferServiceClient()
        self.parent = self.instance.common_project_path(self.project_id)

    def get_transfer_configs(self):
        return self.instance.list_transfer_configs(parent=self.parent)

    def delete_transfers(self, transfers):
        for transfer in transfers:
            self.instance.delete_transfer_config(
                name = transfer.name
            )