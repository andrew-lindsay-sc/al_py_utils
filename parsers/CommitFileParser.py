from helpers.StaticMethods import get_all_clients
from parsers.abstracts.FileParser import FileParser
from clients.BqClient import *

class CommitFileParser(FileParser):
    def __init__(self, commit):
        self.commit = commit
        self.changed_files = self.parse_changed_files()

    def parse_changed_files(self):
        """
            (Commit) -> dict<string, list<string>>
            Parses files modified by the provided merge_commit and returns all SQL files.
        """
        changed_files = {BqClient.Operation.MODIFIED: list(), BqClient.Operation.DELETED: list()}

        for file, detail in self.commit.stats.files.items():
            if file[-4:] != '.sql':
                continue

            if detail["lines"] == detail["deletions"] and detail["insertions"] == 0:
                changed_files[BqClient.Operation.DELETED].append(file)
            # We don't care whether the file was added or updated, it makes no functional difference 
            else:
                changed_files[BqClient.Operation.MODIFIED].append(file)

        return changed_files

    def parse_clients(self):
        """
            dict(str,list<str>) -> dict(str, dict(str,list<str>))
            Transforms the dictionary of changed_files to be broken up by client.
        """
        files_by_client, global_count = self.get_global_updates()

        # overwrite to empty dict if no globals were found
        if global_count == 0:
            files_by_client = {}

        for operation, files in self.changed_files.items():
            for file in files:                    
                # expected path parts: client, dataset, object type, file name
                path_parts = file.replace('infrastructure/gcloud/client/bq/', '').split('/')
                # If the file is 3 or less levels beyond bq folder, it must be a global update (e.g. "ext/view/view_name.sql")
                if len(path_parts) <= 3:
                    continue

                client = path_parts[0]
                if client not in files_by_client:
                    files_by_client[client] = {BqClient.Operation.MODIFIED: list(), BqClient.Operation.DELETED: list()}

                files_by_client[client][operation].append(file)

        return files_by_client

    def get_global_updates(self):
        """
            dict(str,list<str>) -> dict(str, dict(str,list<str>))
            Identifies any modifications which affect all clients and returns this in the format expected by parse_clients.
        """
        files_by_client = { client : {BqClient.Operation.MODIFIED: list(), BqClient.Operation.DELETED: list()} for client in get_all_clients() }
        
        global_count = 0

        for operation, files in self.changed_files.items():
            for file in files:
                path_parts = file.replace('infrastructure/gcloud/client/bq/', '').split('/')
                # If the file is 3 or less levels beyond bq folder, it must be a global update (e.g. "ext/view/view_name.sql")
                if len(path_parts) <= 3:
                    global_count += 1
                    for client in files_by_client:
                        files_by_client[client][operation].append(file)    
                        
        return files_by_client, global_count