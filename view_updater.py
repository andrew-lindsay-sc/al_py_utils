# Must run this before using this script:
#     pip install --upgrade google-cloud-bigquery

from google.cloud import bigquery
import argparse
from helpers.StaticMethods import *

def prepare_args(parser):
    parser.add_argument(
        '-c', help='(Optional) Specify one or more Client(s), if not provided all clients will be updated (e.g. "bbb, pacsun"')
    parser.add_argument(
        '-ic', help='(Optional) Specify one or more Client(s) to ignore, this only works when running for all')
    parser.add_argument(
        '-v', help='(Required) Specify one or more views to be updated (e.g. "ext.vw_name, core.vw_core_thing"')
    # parser.add_argument(
    #     '-cv', action='store_true', help='(Optional) Use the client\'s version of the specified views instead of the product version')
            
def validate_args(args):
    if (not args.v):
        raise Exception("View(s) are required, use -v to set.")
    if (not args.c):
        response = input("No client specified. Run for all? (Y/n): ")
        if not (response.strip().lower() == 'y' or response.strip().lower() == ''):            
            raise Exception("Invalid response, exiting.")

def prepare_bq_client(client_name):
    project_id = "soundcommerce-client-"+client_name
    client = bigquery.Client(project=project_id)
    return client

def get_view_defs(views):
    view_defs = dict()
    for view in views:
        view_file_name = get_bq_path()+"/"+view.replace(".", "/view/")+".sql"
        with open(view_file_name, 'r', newline='\n') as view_file:
            view_defs[view] = view_file.read()

    return view_defs

def update_views(view_string, client_string, ignore_clients_string):
    if not(client_string):
        clients = get_all_clients(ignore_clients_string)
    else:
        clients_temp = client_string.split(",")
        clients = list()
        for client in clients_temp:
            clients.append(client.strip())

    views_temp = view_string.split(",")
    views = list()
    for view in views_temp:
        views.append(view.strip())

    view_defs = get_view_defs(views)

    for client in clients:
        bq_client = prepare_bq_client(client)
        print("Updating "+client)

        project_name = 'soundcommerce-client-'+client

        for view, definition in view_defs.items():
            print("\tUpdating " + view)
            to_create = bigquery.Table(project_name+'.'+view)
            to_create.view_query = \
                definition.replace("${project}", project_name)\
                    .replace("${dataset}", view.split('.')[0].strip())

            bq_client.update_table(
                table=to_create,
                fields=["view_query"]
            )

def main():
    parser = argparse.ArgumentParser()
    prepare_args(parser)
    args = parser.parse_args()
    validate_args(args)     

    update_views(args.v, args.c, args.ic)

if __name__ == "__main__":
    main()