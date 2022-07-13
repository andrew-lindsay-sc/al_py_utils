import copy
from git import DiffIndex
from parsers.abstracts.FileParser import FileParser
from clients.BqClient import *

class DevModeParser(FileParser):
    def __init__(self, diffs: DiffIndex):
        self._diffs = diffs
        self._files_by_client = None
        super().__init__()

    def parse_clients(self):
        """
            None -> dict(str, dict(str,list<str>))
            Transforms the dictionary of changed_files to be broken up by client.
        """
        if self._files_by_client:
            return self._files_by_client
        else:
            changes = { BqClient.Operation.MODIFIED: set(), BqClient.Operation.DELETED: set() }
            self._files_by_client = dict()
            mono_path = get_mono_path()

            for diff in self._diffs:
                client = extract_client_from_path(Path(diff.a_path if diff.change_type == 'D' else diff.b_path))
                if client not in self._files_by_client:
                    self._files_by_client[client] = copy.deepcopy(changes)

                # Include renames so that we clean up after ourselves
                if diff.change_type in ['D', 'R'] and diff.a_path[-4:] == '.sql':
                    self._files_by_client[client][BqClient.Operation.DELETED].add(mono_path+'/'+diff.a_path)
                elif diff.b_path[-4:] == '.sql':
                    self._files_by_client[client][BqClient.Operation.MODIFIED].add(mono_path+'/'+diff.b_path)

            # Apply global changes to all modified clients, then remove the global node
            #   only do this is there are modified clients
            if 'global' in self._files_by_client and len(self._files_by_client.keys()) > 1:
                for diff in self._files_by_client['global']:
                    for key, value in self._files_by_client.items():
                        if key == 'global':
                            continue
                        else:
                            value.add(diff)
            
                self._files_by_client.pop('global')
                        
            return self._files_by_client

    def _parse_changed_files(self):
        return super()._parse_changed_files()