# Must run this before using this script:
#   pip install --upgrade google-cloud-bigquery

from google.cloud import bigquery
from google.cloud import bigquery_datatransfer
import argparse
import json
from pathlib import Path
import time
from dataclasses import dataclass
from helpers.BqClient import BqClient
from helpers.StaticMethods import *

@dataclass
class view_info:
    dataset_id: str
    table_id: str


def prepare_args(parser):
    parser.add_argument(
        '-c', help='(Optional) Specify one or more Client(s), if not provided all clients will be searched')
    parser.add_argument(
        '-ic', help='(Optional) Specify one or more Client(s) to ignore, this only works when running for all')
    parser.add_argument(
        '-v', help='(Required) Specify one or more View(s) to fetch dependents of')
    # parser.add_argument(
    #     '-cv', action='store_true', help='(Optional) Use the client\'s version of the specified views instead of the product version')
            
def validate_args(args):
    if (not args.c):
        response = input("No client specified. Run for all? (Y/n): ")
        if not (response.strip().lower() == 'y' or response.strip().lower() == ''):            
            raise Exception("Invalid response, exiting.")
    if (not args.v):
        raise Exception("No view(s) specified, exiting.")

def prepare_bq_transfer_client(client_name):
    client = bigquery_datatransfer.DataTransferServiceClient()
    project_id = "soundcommerce-client-"+client_name
    parent = client.common_project_path(project_id)
    return client, parent

def get_all_vvws(client, datasets=[]):
    versioned_views = list()

    if len(datasets) == 0:
        datasets = client.list_datasets(max_results=100)

    for dataset in datasets:
        # dataset = client.dataset(dataset_name)
        tables = client.list_tables(dataset.dataset_id)
        for view in filter(lambda c: c.table_type == "VIEW" and c.table_id[0:3] == "vvw", tables):
            versioned_views.append(view)

    return versioned_views

# This method is more "pythonic" than the SQL version, but also much much slower (~5-6x)
def get_dependent_views(dependencies, project_id, bq_client, versioned_views):
    datasets = bq_client.list_datasets(max_results=100)

    for dataset in datasets:
        tables = bq_client.list_tables(dataset.dataset_id)
        for view in filter(lambda c: c.table_type == "VIEW", tables):
            full_view = bq_client.get_table(f"{project_id}.{view.dataset_id}.{view.table_id}")
            vvw_matches = filter(lambda vvw: f"{vvw.dataset_id}.{vvw.table_id}" in full_view.view_query, versioned_views)
            for vvw_match in vvw_matches:
                key = f"{vvw_match.dataset_id}.{vvw_match.table_id}"
                if key in dependencies:
                    dependencies[key].append(f"{view.dataset_id}.{view.table_id}")
                else:
                    dependencies[key] = [f"{view.dataset_id}.{view.table_id}"]

def get_dependent_views_sql(dependencies, bq_client, versioned_views):
    all_views_query = """
        SELECT concat(table_schema, '.', table_name) as full_name, view_definition
        FROM region-us.INFORMATION_SCHEMA.VIEWS
    """
    all_views = bq_client.query(all_views_query).result()

    for view in all_views:
        vvw_matches = filter(lambda vvw: f"{vvw.dataset_id}.{vvw.table_id}" in view.view_definition, versioned_views)
        for vvw_match in vvw_matches:
            key = f"{vvw_match.dataset_id}.{vvw_match.table_id}"
            if key in dependencies:
                dependencies[key].append(view.full_name)
            else:
                dependencies[key] = [view.full_name]

def get_dependent_sqs(client_name, dependencies, versioned_views):
    bq_transfer_client, parent = prepare_bq_transfer_client(client_name)
    transfer_configs = bq_transfer_client.list_transfer_configs(parent=parent)
    for transfer in transfer_configs:
        if not "query" in transfer.params:
            continue

        vvw_matches = filter(lambda vvw: f"{vvw.dataset_id}.{vvw.table_id}" in transfer.params["query"], versioned_views)
        for vvw_match in vvw_matches:
            key = f"{vvw_match.dataset_id}.{vvw_match.table_id}"
            if key in dependencies:
                dependencies[key].append(transfer.display_name)
            else:
                dependencies[key] = [transfer.display_name]


def report_dependents(client, dependencies, all_vvws, report_dependents = False):
    if report_dependents:
        for dependency, dependents in dependencies.items():
            with open('dep_report.csv', 'a') as f:
                for dependent in dependents:
                    f.write(f"\"{client}\", \"{dependent}\", \"{dependency}\"\n")

def print_debug(message, elapsed):
    print(f"\tDEBUG: {message} (client elapsed: {round(elapsed*100.00)/100.00}s)")

def prepare_report_file():
    with open('dep_report.csv', 'w') as f:
        f.write("client_name, dependent_name, dependency_name\n")

def main(debug):
    parser = argparse.ArgumentParser()
    prepare_args(parser)
    args = parser.parse_args()

    args.v = '''ext.vw_marketing_adspend, ext.vw_marketing_campaign_all, ext.vw_marketing_campaign_mapping, 
        ext.vw_marketing_order_attribution, ext.vw_marketing_campaign_override, 
        ext.vw_marketing_source_medium_mapping, ext.vw_marketing_source_medium_mapping_with_stats, 
        ext.vw_marketing_source_medium_override, ext.mv_marketing_campaign_mapping, ext.mv_marketing_source_medium_override
    '''.replace('\n', '')

    validate_args(args)
    views = []
    for view in args.v.split(','):
        parts = view.strip().split('.')
        views.append(view_info(parts[0].strip(), parts[1].strip()))

    clients = []
    if args.c:
        (lambda c: clients.append(c.strip()), args.c.split(","))
    else:
        clients = get_all_clients(args.ic)
    clients.sort()

    prepare_report_file()

    for client in clients:
        t = time.time()
        print(f"Analyzing {client}...")
        bq_client = BqClient(client)

        dependencies = {}
        get_dependent_views_sql(dependencies, bq_client, views)
        if debug:
            print_debug(f"dependent views processed", time.time() - t)

        get_dependent_sqs(client, dependencies, views)
        if debug:
            print_debug(f"dependent SQs processed", time.time() - t)

        elapsed_time = time.time() - t
        print(f"Completed {client} in {round(elapsed_time*100.00)/100.00}s...")
        report_dependents(client, dependencies, views, True)

if __name__ == "__main__":
    main(True)