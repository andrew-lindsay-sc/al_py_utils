from helpers.StaticMethods import print_warn, get_bq_path, object_name_to_type
from helpers.parsers.abstracts.FileParser import FileParser
from helpers.clients.BqClient import *
import copy
import os.path

class CsvFileParser(FileParser):
    def __init__(self, file):
        self.file = file
        if len(self.file) > 0 and not os.path.exists(self.file):
            raise FileNotFoundError(self.file)

    def print_example_file(self):
        print("client_name, operation, object_name")
        print('"sc", "deleted", "dataset.vw_view1"')
        print('"sc", "modified", "dataset.vw_view2"')

    def parse_clients(self):
        bq_path = get_bq_path()
        files_by_client = dict()
        operations = {BqClient.Operation.MODIFIED: list(), BqClient.Operation.DELETED: list()}
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
                full_object_name = clean_columns[2]
                dataset = full_object_name.split('.')[0]
                object_name = full_object_name.split('.')[1]
                object_type = object_name_to_type(object_name)

                file_path = f"{bq_path}/{client_name}/{dataset}/{object_type}/{object_name}.sql"
                if '_0.sql' not in file_path and not os.path.isfile(file_path):
                    print_warn(f"Failed to resolve file path for {client_name}:{object_name}, skipping...")
                    continue

                # initialize empty operations dictionary if client is not present in files_by_client
                if client_name not in files_by_client:
                    files_by_client[client_name] = copy.deepcopy(operations)

                (files_by_client[client_name])[operation].append(file_path)

        return files_by_client