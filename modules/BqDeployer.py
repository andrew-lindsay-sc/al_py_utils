# pip install GitPython

from modules.abstracts.DevToolsModule import DevToolsModule
from parsers.CommitFileParser import *
from parsers.CsvFileParser import *
from parsers.DevModeParser import *
from clients.GitClient import *
from helpers.PrintColors import *
from clients.BqDeploymentClient import *
from helpers.StaticMethods import *
from enum import Enum

class BqDeployer(DevToolsModule):
    class Mode(Enum):
        EXAMPLE = 1
        GIT = 2
        FILE = 3
        DEVELOPMENT = 4

    def __init__(self, mode: Mode, fetch_files_from: str, client_list = '', project_id = None):
        self._mode = mode
        self._git = GitClient(get_mono_path())
        # If Project id is specified, this deployer will only run against one project
        self._project_format = project_id if project_id else "soundcommerce-client-{client}"
        
        # Use master in all but development mode
        if self._mode not in [self.Mode.DEVELOPMENT, self.Mode.EXAMPLE]:
            self._git.switch_to(self._git.master)

        if mode == self.Mode.EXAMPLE:
            CsvFileParser('').print_example_file()
            return
        elif mode == self.Mode.GIT:
            merge_commit = self._git.get_commit_by_sha(fetch_files_from)
            self._parser = CommitFileParser(merge_commit)
        elif mode == self.Mode.FILE:
            self._parser = CsvFileParser(fetch_files_from)   
        elif mode == self.Mode.DEVELOPMENT:
            self._parser = DevModeParser(self._git.get_active_branch_changes())
            re_match = re.match(r'(ACC-[0-9]+)', self._git.original_branch)
            if not re_match and not project_id:
                raise Exception("Branch name is not the standard format, to use dev mode with this branch also specify -p/--project_id")
            elif not project_id: #project_id trumps all as a manual override
                self._project_format = "dev-{client}-"+re_match.group(1)

        if len(self._parser.files_by_client) == 0:
            print("No SQL files found in provided source, exiting...")
        else:
            self._clients = arg_to_list(client_list)
    
    def __enter__(self):
        return self

    def __exit__(self, *args):
        del self
        
    def _print_file_info(self, files, operation):
        """
            Print handler which displays a list of files and the operation done to them.
        """
        for file in files:
            if '/' in file:
                file_parts = file.split('/')

                object_name = file_parts[-1].replace('.sql', '')
                object_type = file_parts[-2]
                dataset = file_parts[-3]
                print_warn(f"({object_type}) {dataset}.{object_name} will be {operation.name.lower()}", indents=1)

            elif '.' in file:
                if file.split('.')[1].split('_')[0] in ('vw', 'vvw'):
                    object_type = 'view'
                else:
                    object_type = 'unknown'

                print_warn(f"({object_type}) {file} will be {operation.name.lower()}")

    def _report_files(self):
        """
            Summarizes the changes made to BQ objects by the commit.
        """
        file_count = 0
        for client, operation in self._parser.files_by_client.items():
            if len(self._clients) > 0 and client not in self._clients:
                continue

            print(f"{client}:")
            self._print_file_info(operation[BqClient.Operation.MODIFIED], BqClient.Operation.MODIFIED)
            self._print_file_info(operation[BqClient.Operation.DELETED], BqClient.Operation.DELETED)
            file_count += len(operation[BqClient.Operation.MODIFIED]) + len(operation[BqClient.Operation.DELETED])

        print(f"Total files to be deployed: {file_count}")

    def _deploy_changes(self):
        """
            Orchestrates deployment of all identified BQ modifications in the commit.
        """
        for client, operation in self._parser.files_by_client.items():
            if len(self._clients) > 0 and client not in self._clients:
                continue

            print(f"Deploying {client}...")
            bq_instance = BqDeploymentClient(client, self._project_format.replace("{client}", client))

            bq_instance.deploy_files(operation[BqClient.Operation.MODIFIED], BqClient.Operation.MODIFIED)
            bq_instance.deploy_files(operation[BqClient.Operation.DELETED], BqClient.Operation.DELETED)

            bq_instance.validate_deployment(operation[BqClient.Operation.DELETED], operation[BqClient.Operation.MODIFIED])

    def execute(self, is_dry_run = True) -> bool:
        if self._mode == self.Mode.EXAMPLE:
            return
            
        self._report_files()
        if not is_dry_run:
            self._deploy_changes()
        else:
            print_info(f"BqDeployer.deploy received is_dry_run, no changes will be made, exiting...")
            return True

        # Restore original state
        if self._mode != self.Mode.DEVELOPMENT:
            self._git.switch_to(self._git.original_head)

        return True
