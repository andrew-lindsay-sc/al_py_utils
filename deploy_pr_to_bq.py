# pip install GitPython

import argparse

from helpers.CommitFileParser import *
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
    parser = CommitFileParser(merge_commit)

    if len(parser.changed_files['deleted']) == 0 and len(parser.changed_files['modified']) == 0:
        print("No SQL files exist in this commit, exiting...")
    else:
        clients = arg_to_list(args.c)

        report_files(parser.files_by_client, clients)
        if(args.go):
            deploy_commit(parser.files_by_client, clients)

    # Restore original state
    git.switch_to(git.original_head)

if __name__ == "__main__":
    main()