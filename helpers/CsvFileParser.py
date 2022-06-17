from requests import head
from helpers.abstracts.FileParser import FileParser
import copy

class CsvFileParser(FileParser):
    def __init__(self, file):
        self.file = file
        return

    def print_example_file(self):
        print("client_name, operation, object_name")
        print('"sc", "deleted", "dataset.vw_view1"')
        print('"sc", "modified", "dataset.vw_view2"')

    def parse_clients(self):
        files_by_client = dict()
        operations = {'modified': list(), 'deleted': list()}
        with open(self.file, 'r') as infile:
            content = infile.read()
            lines = content.split('\n')
            header_skipped = False
            for line in content.split('\n'):
                if not header_skipped and "client_name" in lines[0]:
                    header_skipped = True
                    continue

                if ',' not in line:
                    continue

                columns = line.split(',')
                clean_columns = list()
                for c in columns:
                    clean_columns.append(c.replace('"','').strip())

                client_name = clean_columns[0]
                operation = clean_columns[1]
                object_name = clean_columns[2]

                # initialize empty operations dictionary if client is not present in files_by_client
                if client_name not in files_by_client:
                    files_by_client[client_name] = copy.deepcopy(operations)

                (files_by_client[client_name])[operation].append(object_name)

        return files_by_client