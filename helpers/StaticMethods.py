
import json
from pathlib import Path
from helpers.PrintColors import *

def get_mono_path():
    """
        (None) -> Str
        Returns the default path for the mono repo
    """
    return str(Path.home()) + '/Projects/mono'

def get_bq_path():
    """
        (None) -> Str
        Returns the default path for the bq folder in the mono repo
    """
    return get_mono_path()+'/infrastructure/gcloud/client/bq'

def get_all_clients(ignore_clients_string = ''):
    """
        (Str(optional)) -> list<str>
        Returns a list of all SC clients, ignoring any specified
    """
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

def arg_to_list(arg, seperator = ','):
    """
        (Str) -> list<str>
        Tokenizes the provided argument string into a list based on separator (default ",")
    """
    arg_list = list()
    if not arg:
        return arg_list

    for arg_value in arg.split(seperator):
        arg_list.append(arg_value.strip())
    
    return arg_list

def paths_to_sql_names(paths):
    """
        (list<str>) -> list<str>
        Converts a list of file paths into a list of SQL object names.
    """
    for path in paths:
        parts = path.split('/')        
        yield f"{parts[-3]}.{parts[-1][:-4]}"

def print_success(message, indents = 0):
    """
        (Str) -> None
        Prints "Success: " in green, followed by the provided message.
    """
    print_color(('\t'*indents)+"Success:", PrintColors.OKGREEN, end=' ')
    print(message)

def print_fail(message, indents = 0):
    """
        (Str) -> None
        Prints "Fail: " in red, followed by the provided message.
    """
    print_color(('\t'*indents)+"Fail:", PrintColors.FAIL, end=' ')
    print(message)

def print_warn(message, indents = 0):
    """
        (Str) -> None
        Prints "Warning: " in yellow, followed by the provided message.
    """
    print_color(('\t'*indents)+"Warning:", PrintColors.WARNING, end=' ')
    print(message)

def print_info(message, indents = 0):
    """
        (Str) -> None
        Prints "Info: " in cyan, followed by the provided message.
    """
    print_color(('\t'*indents)+"Info:", PrintColors.OKCYAN, end=' ')
    print(message)

def object_name_to_type(name):
    if '_' not in name:
        return 'unknown'

    object_prefix = name.split('_')[0]
    if object_prefix in ('vvw', 'vw'):
            return 'view'
    elif object_prefix == 'mv':
        return 'materialized view'
    else:
        return 'unknown'
    # TODO: make this less bad

def is_quoted(text):
    return text[0] == "'" and text[-1] == "'"