# pip install GitPython

import argparse

from helpers.GitClient import *
from helpers.PrintColors import *
from helpers.BqDeploymentClient import *
from helpers.StaticMethods import *

def prepare_args(parser):
    parser.add_argument(
        '-sha', help='(Required) Specify the SHA of a PR merge commit to deploy')
    parser.add_argument(
        '-go', action='store_true', help='(Optional) Deploy the specified commit, otherwise changes will be reported but not deployed')
    parser.add_argument(
        '-c', help='(Optional) Restrict clients to be modified to the specified list (e.g. "truthbar, rainbow2"')

def validate_args(args):
    if (not args.sha):
        raise Exception("SHA is required, use -sha to set.")

def handle_args():
    """
        Parses and validates arguments passed in by the command line invocation.
    """
    parser = argparse.ArgumentParser()
    prepare_args(parser)
    args = parser.parse_args()
    validate_args(args)  

    return args

def get_global_updates(changed_files):
    """
        dict(str,list<str>) -> dict(str, dict(str,list<str>))
        Identifies any modifications which affect all clients and returns this in the format expected by parse_clients.
    """
    files_by_client = { client : {'modified': list(), 'deleted': list()} for client in get_all_clients() }
    
    global_count = 0

    for operation, files in changed_files.items():
        for file in files:
            path_parts = file.replace('infrastructure/gcloud/client/bq/', '').split('/')
            # If the file is 3 or less levels beyond bq folder, it must be a global update (e.g. "ext/view/view_name.sql")
            if len(path_parts) <= 3:
                global_count += 1
                for client in files_by_client:
                    files_by_client[client][operation].append(file)    
    return files_by_client, global_count

def parse_clients(changed_files):
    """
        dict(str,list<str>) -> dict(str, dict(str,list<str>))
        Transforms the dictionary of changed_files to be broken up by client.
    """
    files_by_client, global_count = get_global_updates(changed_files)

    # overwrite to empty dict if no globals were found
    if global_count == 0:
        files_by_client = {}

    for operation, files in changed_files.items():
        for file in files:                    
            # expected path parts: client, dataset, object type, file name
            path_parts = file.replace('infrastructure/gcloud/client/bq/', '').split('/')
            # If the file is 3 or less levels beyond bq folder, it must be a global update (e.g. "ext/view/view_name.sql")
            if len(path_parts) <= 3:
                continue

            client = path_parts[0]
            if client not in files_by_client:
                files_by_client[client] = {'modified': list(), 'deleted': list()}

            files_by_client[client][operation].append(file)

    return files_by_client

def print_file_info(files, operation):
    """
        Print handler which displays a list of files and the operation done to them.
    """
    for file in files:
        file_parts = file.split('/')

        object_name = file_parts[-1].replace('.sql', '')
        object_type = file_parts[-2]
        dataset = file_parts[-3]

        print(f"\t({object_type}) {dataset}.{object_name} ", end='')
        print_color(f"will be {operation}", PrintColors.WARNING)

def report_files(report_files, clients):
    """
        Summarizes the changes made to BQ objects by the commit.
    """
    file_count = 0
    for client, operation in report_files.items():
        if len(clients) > 0 and client not in clients:
            continue

        print(f"{client}:")
        print_file_info(operation['modified'], 'modified')
        print_file_info(operation['deleted'], 'deleted')
        file_count += len(operation['modified']) + len(operation['deleted'])

    print(f"Total files to be deployed: {file_count}")

def parse_changed_files(merge_commit):
    """
        Parses files modified by the provided merge_commit and returns all SQL files.
    """
    changed_files = {'modified': list(), 'deleted': list()}

    for file, detail in merge_commit.stats.files.items():
        if file[-4:] != '.sql':
            continue

        if detail["lines"] == detail["deletions"] and detail["insertions"] == 0:
            changed_files['deleted'].append(file)
        # We don't care whether the file was added or updated, it makes no functional difference 
        else:
            changed_files['modified'].append(file)

    return changed_files

def deploy_commit(files_by_client, clients):
    """
        Orchestrates deployment of all identified BQ modifications in the commit.
    """
    for client, operation in files_by_client.items():
        if len(clients) > 0 and client not in clients:
            continue

        print(f"Deploying {client}...:")
        bq_instance = BqDeploymentClient(client)

        bq_instance.deploy_files(operation['modified'], 'modified')
        bq_instance.deploy_files(operation['deleted'], 'deleted')

        bq_instance.validate_deployment(operation['deleted'], operation['modified'])

def main():
    args = handle_args()
    git = GitClient(get_mono_path())

    # Work from Master as we should only be deployed merged work
    git.switch_to(git.master)

    merge_commit = git.get_commit_by_sha(args.sha)
    changed_files = parse_changed_files(merge_commit)

    if len(changed_files['deleted']) == 0 and len(changed_files['modified']) == 0:
        print("No SQL files exist in this commit, exiting...")
    else:
        files_by_client = parse_clients(changed_files)

        clients = arg_to_list(args.c)

        report_files(files_by_client, clients)
        if(args.go):
            deploy_commit(files_by_client, clients)

    # Restore original state
    git.switch_to(git.original_head)

if __name__ == "__main__":
    main()