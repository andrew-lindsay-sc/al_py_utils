import copy
import re
from google.cloud import bigquery_datatransfer
from google.cloud.bigquery_datatransfer_v1.types.transfer import TransferConfig, UserInfo
from google.cloud import bigquery_datatransfer_v1
from google.protobuf import field_mask_pb2

from clients.BqClient import *
from helpers.StaticMethods import *

class BqTransferClient(BqClient):
    """Helper class (child of BqClient) designed to help with transferconfig operations."""
    def __init__(self, client_name):
        BqClient.__init__(self, client_name, True)
        self.instance = bigquery_datatransfer.DataTransferServiceClient()
        self.parent = self.instance.common_project_path(self.project_id)
        self._transfer_configs = self.instance.list_transfer_configs(parent=self.parent)

    def get_transfer_configs(self):
        return self._transfer_configs

    def delete_transfers(self, transfers):
        for transfer in transfers:
            self.instance.delete_transfer_config(
                name = transfer.name
            )

    def _validate_service_account_update(self, transfer_config: TransferConfig, service_account_name: str):
        email = transfer_config.owner_info.email

        if email == service_account_name:
            # print_info(f"Skipping \"{transfer_config.display_name}\"; service account matches expected value.")
            return False, transfer_config
        elif len(email) > 0 and not re.search(".+@soundcommerce.com", email):
            # print_info(f"Skipping \"{transfer_config.display_name}\"; current email \"{email}\" does not match soundcommerce email pattern.")
            return False, transfer_config

        return True, transfer_config

    # TODO: Support specifying different fields per SQ
    def get_config_updates(self, updates: dict[str,str]): 
        configs = self._transfer_configs

        targeted_update = False

        # Handle display_name filtering
        if 'display_name' in updates:
            targeted_update = True
            configs = list(filter(lambda config: config.display_name == updates['display_name'], configs))
            updates.pop('display_name')

        for config in configs:        
            # Ensure we have all the data we need
            config = self.instance.get_transfer_config(
                bigquery_datatransfer_v1.GetTransferConfigRequest(name=config.name)
            )

            # Handle Service account name
            config_updates = copy.deepcopy(updates)
            if not targeted_update and 'service_account_name' in config_updates:
                is_account_update_valid, config = self._validate_service_account_update(config, config_updates['service_account_name'])
                if not is_account_update_valid:
                    del config_updates['service_account_name']
                    if len(config_updates) == 0:
                        continue 

            paths = list(updates.keys())
            if len(paths) == 0:
                continue

            update_config = {
                "transfer_config": config,
                "update_mask": field_mask_pb2.FieldMask(paths=paths)
            }
            for field, value in updates.items():
                update_config[field] = value

            yield update_config

    def update_config_fields(self, updates: dict[str, str]) -> None:
        to_update = list(self.get_config_updates(updates))

        for update in to_update:
            self.instance.update_transfer_config(update)

        # Refresh _transfer_configs after an update
        self._transfer_configs = self.instance.list_transfer_configs(parent=self.parent)

    def _get_current_value(self, config: TransferConfig, key: str):
        if key == 'service_account_name':
            return config.owner_info.email
        elif hasattr(TransferConfig, key):
            return getattr(TransferConfig, key)
        else:
            raise Exception(f"Provided key {key} not found in TransferConfig")

    def update_transfers(self, updates: dict[str,str], dry_run):
        if not dry_run:
            self.update_config_fields(updates)
        else:
            update_configs = list(self.get_config_updates(updates))
            for config in update_configs:
                print_info(f"Updates for {config['transfer_config'].display_name}:", 1)
                
                for key, new_value in config.items():
                    # Don't print the whole thing, just updates
                    if key in ['transfer_config', 'update_mask']:
                        continue
                    else:
                        current_value = self._get_current_value(config['transfer_config'], key)
                        print('\t\t'+f"{key}: {current_value} -> {new_value}")