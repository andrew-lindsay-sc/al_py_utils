# Must run this before using this script:
#     pip install --upgrade google-cloud-bigquery-datatransfer

from google.cloud import bigquery_datatransfer
from google.protobuf.field_mask_pb2 import FieldMask
import argparse
import csv
from dataclasses import dataclass
from helpers.StaticMethods import get_all_clients
from helpers.StaticMethods import print_info
from helpers.BqTransferClient import BqTransferClient
from google.cloud.bigquery_datatransfer_v1.types.transfer import TransferConfig, UserInfo

@dataclass
class query_info:
    display_name: str
    schedule: str

def parse_query_list(queries_list, schedule):
    scheduled_query_names = queries_list.split(", ")
    for name in scheduled_query_names:
        yield query_info(display_name=name, schedule=schedule)

def parse_query_file(query_file):
    with open(query_file, newline='') as f:
        reader = csv.reader(f)
        # Skip first row as it is the headers
        for row in reader:
            if(row[0]=="query_display_name"): continue
            yield query_info(display_name=row[0], schedule=row[1])

def prepare_args(parser):
    parser.add_argument(
        '-c', help='(Required) Specify a Client, with no sq value this script shows the names of all SQs')
    parser.add_argument(
        '-ic', help='(Required) Specify a client(s) to ignore as a comma separated list')
    parser.add_argument(
        '-sq', help='(Optional) Specify one or more Scheduled Query Display Names. This can be Comma separated or json. If not specified, script runs for all')
    parser.add_argument('-sfgen', action='store_true',
                        help='(Optional) Generate a sample csv file with schedules and exit.')
    parser.add_argument(
        '-sf', help='(Optional) Specify a csv file with schedules.')
    parser.add_argument(
        '-s', help='(Optional) Specify a new schedule to set for the specified queries')
    parser.add_argument(
        '-u', help='(Optional) Specify a new user account to set for the specified queries')
    parser.add_argument(
        '-v', help='(Optional) Verbose mode, display extra information about scheduled queries')
    parser.add_argument(
        '-n',  action='store_true',
        help='(Optional) Name only for query information output')
    parser.add_argument(
        '-sa',  action='store_true',
        help='(Optional) Set any scheduled query which is not using the service account to do so.')
    parser.add_argument(
        '-go',  action='store_true',
        help='(Optional) Must be specified to cause changes, otherwise dryrun.')

def generate_sample_file():
    f = open("query_schedule.csv", "w")
    f.write("query_display_name, schedule\n")
    f.write("Materialize dataset.view, every day 07:00\n")
    f.write("Materialize dataset2.view2, every day 09:30\n")
    f.close()

def validate_args(args):
    if (not args.c and not args.sa):
        raise Exception("Client is required, use -c to set.")
    elif(args.s and not args.sq and not args.sf):
        raise Exception(
            "Schedule can only be set if one or more SQ Display Names are set with -sq.")
    elif(args.sf and args.sq):
        raise Exception(
            "Scheduled Queries cannot be provided in both list and file format")
    elif(args.sq and not args.s):
        raise Exception("Schedule (-s) is required when using the -sq flag")

def print_transfers(transfer_configs, name_only, verbose):
    for transfer in transfer_configs:
        print(transfer.display_name+":")
        if(not name_only):
            print("\tschedule: " + transfer.schedule)
        if(verbose):
            print("\tname: " + transfer.name)
            print("\tquery: " +
                  transfer.params["query"].replace("\n", "\n\t\t"))

def update_transfers(scheduled_queries, transfer_configs, client):
    if (len(scheduled_queries) > 0):
        print("Info: Updating schedules...")
        field_mask = FieldMask()
        field_mask.FromJsonString("schedule")

        for transfer in transfer_configs:
            matching_info = next(x for x in scheduled_queries if x.display_name == transfer.display_name)
            if (transfer.schedule == matching_info.schedule):
                print("Skipping '" + transfer.display_name +
                      "', schedule already matches.")
                continue
            transfer.schedule = matching_info.schedule

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
    set_service_account = args.sa
    sample_file_gen = args.sfgen

    if not set_service_account:
        client = args.c
        bq_client = BqTransferClient(client)
        transfer_configs = bq_client.get_transfer_configs()
        if (sample_file_gen):
            generate_sample_file()
            return(0)

        scheduled_queries = list()

        if(args.sq):
            scheduled_queries = list(parse_query_list(args.sq, args.s))
        elif(args.sf):
            scheduled_queries = list(parse_query_file(args.sf))
            
        transfer_configs = list(
            filter(
                lambda c: c.display_name in (x.display_name for x in scheduled_queries), 
                transfer_configs)
        )

        print_transfers(transfer_configs, args.n, args.v)
        update_transfers(scheduled_queries, transfer_configs, bq_client)
    else:
        if args.c:
            clients = [args.c]
        else:
            clients = get_all_clients(args.ic)
        
        for client in clients:
            print_info(f"Checking client {client}...")
            bq_client = BqTransferClient(client)
            updates = {"service_account_name": 
                        f"bq-scheduled-query-runner@soundcommerce-client-{client}.iam.gserviceaccount.com"}

            bq_client.update_transfers(updates, not args.go)        

if __name__ == "__main__":
    main()