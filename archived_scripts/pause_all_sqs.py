# Must run this before using this script:
#     pip install --upgrade google-cloud-bigquery-datatransfer

from google.cloud import bigquery_datatransfer
from google.protobuf.field_mask_pb2 import FieldMask
import argparse

def prepare_args(parser):
    parser.add_argument(
        '-c', help='(Required) Specify the client to pause all SQs for.')

def validate_args(args):
    if (not args.c):
        raise Exception("Client is required, use -c to set.")

def prepare_client(client_name):
    client = bigquery_datatransfer.DataTransferServiceClient()
    project_id = "soundcommerce-client-"+client_name
    parent = client.common_project_path(project_id)
    return client, parent

def update_transfers(scheduled_queries, client):
    if (len(scheduled_queries) > 0):
        print("Info: Updating schedules...")
        field_mask = FieldMask()
        field_mask.FromJsonString("disabled")

        for transfer in scheduled_queries:            
            transfer.disabled = True

            # Write the change to BQ
            client.update_transfer_config(
                bigquery_datatransfer.UpdateTransferConfigRequest(
                    transfer_config=transfer,
                    update_mask=field_mask
                )
            )

def main():
    parser = argparse.ArgumentParser()
    prepare_args(parser)
    args = parser.parse_args()

    validate_args(args) 
    client, parent = prepare_client(args.c)
    transfer_configs = client.list_transfer_configs(parent=parent)
        
    scheduled_queries = list(
        filter(
            lambda c: not c.disabled, 
            transfer_configs))

    update_transfers(scheduled_queries, client)

if __name__ == "__main__":
    main()