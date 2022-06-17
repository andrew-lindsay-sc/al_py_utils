
import json
from pathlib import Path
from PrintColors import *

def get_mono_path():
    return str(Path.home()) + '/Projects/mono'

def get_bq_path():
    return get_mono_path()+'/infrastructure/gcloud/client/bq'

def get_all_clients(ignore_clients_string = ''):
    ignore_clients = list()
    if ignore_clients_string:
        ignore_clients_temp = ignore_clients_string.split(",")
        for ignore_client in ignore_clients_temp:
            ignore_clients.append(ignore_client.strip())
        
    clients_json_path = get_bq_path()+'/../clients.json'
    with open(clients_json_path, 'r', newline='\n') as clients_file:
        clients_json = json.load(clients_file)
        client_list = list()
        for client in clients_json:
            if len(ignore_clients) == 0 or client['client_name'] not in ignore_clients:
                client_list.append(client['client_name'])
        return client_list

def arg_to_list(arg):
    arg_list = list()
    for arg_value in arg.split(','):
        arg_list.append(arg_value.strip())
    
    return arg_list

def paths_to_sql_names(paths):
    for path in paths:
        parts = path.split('/')
        yield f"{parts[-3]}.{parts[-1][:-4]}"

def print_success(message):
    print_color("\tSuccess:", PrintColors.OKGREEN, end=' ')
    print(message)

def print_fail(message):
    print_color("\tSuccess:", PrintColors.FAIL, end=' ')
    print(message)

def print_warn(message):
    print_color("\tSuccess:", PrintColors.WARNING, end=' ')
    print(message)
