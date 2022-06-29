# pip install GitPython

import argparse

from helpers.CommitFileParser import *
from helpers.CsvFileParser import *
from helpers.GitClient import *
from helpers.PrintColors import *
from helpers.BqDeploymentClient import *
from helpers.StaticMethods import *
from helpers.abstracts.FileParser import FileParser

class BqDeployer:
    def __init__():
        

def prepare_args(parser):
    parser.add_argument(
        '-sha', help='(Optional) Specify the SHA of a PR merge commit to deploy')
    parser.add_argument(
        '-file', help='(Optional) Specify a file of modifications to deploy, use -exampleFile for usage')
    parser.add_argument(
        '-exampleFile', action='store_true', help='(Optional) Displays expected format for a modification file, then exits')
    parser.add_argument(
        '-go', action='store_true', help='(Optional) Deploy the specified commit, otherwise changes will be reported but not deployed')
    parser.add_argument(
        '-c', help='(Optional) Restrict clients to be modified to the specified list (e.g. "truthbar, rainbow2"')

def validate_args(args):
    if (not (args.sha or args.file)):
        raise Exception("At least one of -sha or -file is required.")

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
        if '/' in file:
            file_parts = file.split('/')

            object_name = file_parts[-1].replace('.sql', '')
            object_type = file_parts[-2]
            dataset = file_parts[-3]
            print_warn(f"({object_type}) {dataset}.{object_name} will be {operation}")

        elif '.' in file:
            if file.split('.')[1].split('_')[0] in ('vw', 'vvw'):
                object_type = 'view'
            else:
                object_type = 'unknown'

            print_warn(f"({object_type}) {file} will be {operation}")

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

        print(f"Deploying {client}...")
        bq_instance = BqDeploymentClient(client)

        bq_instance.deploy_files(operation['modified'], 'modified')
        bq_instance.deploy_files(operation['deleted'], 'deleted')

        bq_instance.validate_deployment(operation['deleted'], operation['modified'])

def main():
    args = handle_args()
    git = GitClient(get_mono_path())

    # Work from Master as we should only be deployed merged work
    git.switch_to(git.master)

    if args.exampleFile:
        CsvFileParser('').print_example_file()
        return
    elif args.sha:
        merge_commit = git.get_commit_by_sha(args.sha)
        parser = CommitFileParser(merge_commit)
    elif args.file:
        parser = CsvFileParser(args.file)    

    if len(parser.files_by_client) == 0:
        print("No SQL files found in provided source, exiting...")
    else:
        clients = arg_to_list(args.c)

        report_files(parser.files_by_client, clients)
        if(args.go):
            deploy_commit(parser.files_by_client, clients)
        else:
            print_info(f"-go was not specified, no changes will be made, exiting...")
            return

    # Restore original state
    git.switch_to(git.original_head)

if __name__ == "__main__":
    main()