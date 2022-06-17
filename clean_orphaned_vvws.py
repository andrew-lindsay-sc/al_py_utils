# Must run this before using this script:
#   pip install --upgrade google-cloud-bigquery

import os
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer
import argparse
import json
from pathlib import Path
import time
from dataclasses import dataclass

# Helper dataclass (struct) for the non-bq method (see main)
# This allows usage of orphaned data later to not care which method we used
@dataclass
class bq_table_lite:
    dataset_id: str
    table_id: str

# Define all accepted arguments
def prepare_args(parser):
    parser.add_argument(
        '-c', help='(Optional) Specify one or more Client(s), if not provided all clients will be updated (e.g. "bbb, pacsun"')
    parser.add_argument(
        '-ic', help='(Optional) Specify one or more Client(s) to ignore, this only works when running for all')
    # Not yet implemented
    # parser.add_argument(
    #     '-go', help='(Optional) Executes dropping of identified views')
    parser.add_argument(
        '-report', help='(Optional) Skip analysis and use an existing report file')
            
def validate_args(args):
    if (not args.c):
        response = input("No client specified. Run for all? (Y/n): ")
        if not (response.strip().lower() == 'y' or response.strip().lower() == ''):            
            raise Exception("Invalid response, exiting.")

# Gets the base path for mono bq config, assumes standard setup
def get_bq_path():
    home_dir = Path.home()
    return str(home_dir) + '/Projects/mono/infrastructure/gcloud/client/bq'

# creates a BQ connection
def prepare_bq_client(client_name):
    project_id = "soundcommerce-client-"+client_name
    client = bigquery.Client(project=project_id)
    return client

# creates a BQ transfer connection, this different type is required for SQs
def prepare_bq_transfer_client(client_name):
    client = bigquery_datatransfer.DataTransferServiceClient()
    project_id = "soundcommerce-client-"+client_name
    parent = client.common_project_path(project_id)
    return client, parent

# Fetch all clients from clients.json
# TODO: Should probably ignore inactive
def get_all_clients(ignore_clients_string):
    # Parse ignore clients if specified
    ignore_clients = list()
    if ignore_clients_string:
        ignore_clients_temp = ignore_clients_string.split(",")
        for ignore_client in ignore_clients_temp:
            ignore_clients.append(ignore_client.strip())
        
    # Read clients.json and populate a list to return, respecting ignore clients
    clients_json_path = get_bq_path()+'/../clients.json'
    with open(clients_json_path, 'r', newline='\n') as clients_file:
        clients_json = json.load(clients_file)
        client_list = list()
        for client in clients_json:
            if len(ignore_clients) == 0 or client['client_name'] not in ignore_clients:
                client_list.append(client['client_name'])
        return client_list

# Fetch all versioned views for the specified client, optionally look only in the specified dataset(s)
def get_all_vvws(client, datasets=[]):
    versioned_views = list()

    # If no datasets were specified, get all of them
    if len(datasets) == 0:
        datasets = client.list_datasets(max_results=100)

    # For each dataset, find all views where the name starts with vvw, put them in a list
    for dataset in datasets:
        tables = client.list_tables(dataset.dataset_id)
        for view in filter(lambda c: c.table_type == "VIEW" and c.table_id[0:3] == "vvw", tables):
            versioned_views.append(view)

    return versioned_views

# Fetch dependencies for all VVWs in the current client
def get_dependent_views_sql(dependencies, bq_client, versioned_views):

    # Pull all the view definitions at once
    all_views_query = """
        SELECT concat(table_schema, '.', table_name) as full_name, view_definition
        FROM region-us.INFORMATION_SCHEMA.VIEWS
    """
    all_views = bq_client.query(all_views_query).result()

    # For each view in this client's project:
    #   1. Identify any versioned views which are referenced in the definition
    #   2. Insert them to a dict<string, list<string>>. Key is versioned view's name, value is a list of dependents
    for view in all_views:
        vvw_matches = filter(lambda vvw: f"{vvw.dataset_id}.{vvw.table_id}" in view.view_definition, versioned_views)
        for vvw_match in vvw_matches:
            key = f"{vvw_match.dataset_id}.{vvw_match.table_id}"
            if key in dependencies:
                dependencies[key].append(view.full_name)
            else:
                dependencies[key] = [view.full_name]

    # Don't return anything, python passes dicts by reference

def get_dependent_sqs(client_name, dependencies, versioned_views):
    # Prepare the transfer specific client
    bq_transfer_client, parent = prepare_bq_transfer_client(client_name)

    # Pull all data transfer configs
    transfer_configs = bq_transfer_client.list_transfer_configs(parent=parent)
  
    # For each transfer config in this client's project:
    #   1. Identify any versioned views which are referenced in the query
    #   2. Insert them to a dict<string, list<string>>. Key is versioned view's name, value is a list of dependents  
    for transfer in transfer_configs:

        # We only care about SQs here, skip otherwise
        if not "query" in transfer.params:
            continue

        vvw_matches = filter(lambda vvw: f"{vvw.dataset_id}.{vvw.table_id}" in transfer.params["query"], versioned_views)
        for vvw_match in vvw_matches:
            key = f"{vvw_match.dataset_id}.{vvw_match.table_id}"
            if key in dependencies:
                dependencies[key].append(transfer.display_name)
            else:
                dependencies[key] = [transfer.display_name]

    # Don't return anything, python passes dicts by reference

# Report all orphaned views, and optionally dependencies identified
def report_orphans(client, orphans, dependencies, report_dependents = False):
    
    # Report (print) all vvws which did find dependencies, and what those deps are
    if report_dependents:
        for vvw, dependents in dependencies.items():
            print(f"Versioned View {vvw} has these dependents:")
            for dependent in dependents:
                print(f"\t{dependent.dataset_id}.{dependent.table_id}")

            print('')

    # Append to the report file for the current client's identified orphaned views
    if len(orphans) > 0:
        with open('orphaned_vvw_report.csv', 'a') as f:
            for vvw in orphans:
                if vvw not in dependencies:
                    f.write(f"\"{client}\", \"{vvw.dataset_id}.{vvw.table_id}\"\n")

# Delete orphaned views in the current client's local mono BQ folder
# ... I really thought this would take more code
def clean_local_files(orphans, client_name):
    client_path = f"{get_bq_path()}/{client_name}"
    for orphan in orphans:
        to_clean = f"{client_path}/{orphan.dataset_id}/view/{orphan.table_id}.sql"
        if os.path.exists(to_clean):
            os.remove(to_clean)

# Read the report file provided with `-report "file_name.csv"`
def get_orphans_from_report_file(report_path, client_name):
    orphans = []
    with open(report_path, 'r') as f:
        for line in f:
            # Expected format is: "client_name", "dataset.orphaned_vvw_name"
            raw_parts = line.split(",")
            parts = []
            # Clean up the text
            for part in raw_parts:
                parts.append(part.replace('"', '').replace('\n','').strip())

            # If this is the header row or not yet at the right client, skip
            if parts[0] == "client_name" or parts[0] < client_name:
                continue

            # If we have reached the end of this client's data, exit
            if parts[0] > client_name:
                break
            
            object_name_parts = parts[1].split('.')
            orphans.append(
                # Make use of the table meta spoofing dataclass
                bq_table_lite(
                    dataset_id = object_name_parts[0],
                    table_id = object_name_parts[1]
                )
            )

    return orphans

# Helper for formatting debug output
def print_debug(message, elapsed):
    print(f"\tDEBUG: {message} (client elapsed: {round(elapsed*100.00)/100.00}s)")

# Create/overwrite the report csv, populate the header
def prepare_report_file():
    with open('orphaned_vvw_report.csv', 'w') as f:
        f.write("client_name, versioned_view_name\n")

# Handle argument setup and processing
def prepare_args():
    parser = argparse.ArgumentParser()
    prepare_args(parser)
    args = parser.parse_args()
    validate_args(args)
    return args

# Main BQ Processing orchestrator
# Identify dependencies and orphans for the given client
def process_client_dependencies(debug, client):
    t = time.time()
    print(f"Analyzing {client}...")
    bq_client = prepare_bq_client(client)

    versioned_views = get_all_vvws(bq_client)
    if debug:
        print_debug(f"vvws fetched", time.time() - t)

    dependencies = {}
    get_dependent_views_sql(dependencies, bq_client, versioned_views)
    if debug:
        print_debug(f"dependent views processed", time.time() - t)

    get_dependent_sqs(client, dependencies, versioned_views)
    if debug:
        print_debug(f"dependent SQs processed", time.time() - t)

    elapsed_time = time.time() - t
    print(f"Completed {client} in {round(elapsed_time*100.00)/100.00}s...")

    orphans = list(filter(lambda vvw: f"{vvw.dataset_id}.{vvw.table_id}" not in dependencies, versioned_views))
    report_orphans(client, orphans, dependencies)

    return orphans

def main(debug):

    args = prepare_args()

    clients = []
    # Parse clients passed in if there are any, otherwise get all of them
    if args.c:
        (lambda c: clients.append(c.strip()), args.c.split(","))
    else:
        clients = get_all_clients(args.ic)        
    clients.sort()

    # If no report file has been passed in, prepare the file for generation
    # example report structure...
    #   client_name, versioned_view_name
    #   "bala", "ext.vvw_customer_metrics_0"
    if not args.report:
        prepare_report_file()

    # Main logical loop, iterates on clients since BQ is inherently project scoped
    for client in clients:
        if not args.report:
            # If we're not working off an existing report, use BQ to generate one
            orphans = process_client_dependencies(debug, client)
        else:
            # If we are working off an existing report, read it
            orphans = get_orphans_from_report_file(args.report, client)

        if len(orphans) > 0:
            # If orphans exists, remove them from local repo
            clean_local_files(orphans, client)


if __name__ == "__main__":
    main(True)