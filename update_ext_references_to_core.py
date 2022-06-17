# Must run this before using this script:
#   pip install --upgrade google-cloud-bigquery

import json
import os
import argparse
from pathlib import Path

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

# Delete orphaned views in the current client's local mono BQ folder
# ... I really thought this would take more code
def clean_local_files(orphans, client_name):
    client_path = f"{get_bq_path()}/{client_name}"
    for orphan in orphans:
        to_clean = f"{client_path}/{orphan.dataset_id}/view/{orphan.table_id}.sql"
        if os.path.exists(to_clean):
            os.remove(to_clean)

# Helper for formatting debug output
def print_debug(message, elapsed):
    print(f"\tDEBUG: {message} (client elapsed: {round(elapsed*100.00)/100.00}s)")

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

# Handle argument setup and processing
def handle_args():
    parser = argparse.ArgumentParser()
    prepare_args(parser)
    args = parser.parse_args()
    #validate_args(args)
    return args

def main(debug):

    args = handle_args()
    clients = get_all_clients(args.ic)

    moved_views = [
        'vw_marketing_adspend',
        'vw_marketing_campaign_all',
        'vw_marketing_campaign_mapping',
        'vw_marketing_order_attribution',
        'vw_marketing_campaign_override',
        'vw_marketing_source_medium_mapping',
        'vw_marketing_source_medium_mapping_with_stats',
        'vw_marketing_source_medium_override'
    ]
    file_count = 0
    updated_file_count = 0

    for root, dir, files in os.walk(get_bq_path()):
        for file in filter(lambda f: '.sql' in f, files):
            last_folder = list(root.split('/'))[-1]
            with open(f"{root}/{file}", 'r') as read_file:
                content = read_file.read()
                original_content = content

                for view in moved_views:
                    if f"ext.{view}" in content:
                        content = content.replace(f"ext.{view}", f"core.{view}")

                    pattern = "${dataset}." + view 
                    if last_folder == 'ext' and pattern in content:
                        content = content.replace(pattern, f"core.{view}")

                if content != original_content:
                    with open(f"{root}/{file}", 'w') as write_file:
                        write_file.write(content)
                    updated_file_count += 1

            file_count += 1

    print(f"Done, processed {file_count} files, updated {updated_file_count}")

if __name__ == "__main__":
    main(True)