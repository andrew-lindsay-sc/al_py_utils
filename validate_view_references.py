from dataclasses import dataclass
import os
import re
from unicodedata import name
from xmlrpc.client import Boolean

from helpers.StaticMethods import *
from helpers.PrintColors import *

class SqlObject:
    def __init__(
        self,
        object_name: str, 
        object_type: str,
        dataset: str, 
        client_name: str, 
        file_path: str, 
        bq_project = '', 
        definition = ''
    ):
        self.object_name = object_name
        self.object_type = object_type
        self.dataset = dataset
        self.client_name = client_name
        self.bq_project = bq_project if len(bq_project) > 0 else 'soundcommerce-client-'+client_name
        self.file_path = file_path
        self._definition = definition
        self.fully_qualified_name = f"{self.bq_project}.{self.dataset}.{self.object_name}"

    def __eq__(self, obj):
        if not(isinstance(obj, SqlObject)):
            return False

        if self.fully_qualified_name == obj.fully_qualified_name:
            return True
        else:
            is_equal = ((self.client_name == 'bq') ^ (obj.client_name == 'bq')) 
            is_equal &= (self.dataset == obj.dataset or ((self.dataset == '') ^ (obj.dataset == ''))) 
            is_equal &= self.object_type == obj.object_type

            if len(self.object_name) == 0 or len(obj.object_name) == 0:
                return
            self_compare_name = self.object_name[:-1] if self.object_name[-1] == '*' else self.object_name
            obj_compare_name = obj.object_name[:-1] if obj.object_name[-1] == '*' else obj.object_name
                
            is_equal &= self_compare_name == obj_compare_name
            return is_equal

    def _init_definition(self):
        try:
            with open(self.file_path, 'r') as f:
                self._definition = f.read()
                return True
        except:
            return False

    def _set_definition(self, value: str):
        self._definition = value

    def _get_definition(self):
        if len(self._definition) == 0:
            if not(self._init_definition()):
                raise Exception(f"Failed to read definition from file '{self.file_path}'")
            
        return self._definition

    definition = property(
        fget=_get_definition,
        fset=_set_definition,
        doc="The definition property."
    )

    def __hash__(self):
        return hash(self.fully_qualified_name)

def paths_to_sql_objects(paths: list[Path]):
    sql_objects = list()
    for path in paths:
        sql_objects.append(
            SqlObject(
                object_name = path.parts[-1].split('.')[0], 
                object_type = path.parts[-2],
                dataset = path.parts[-3],
                client_name = path.parts[-4],
                file_path = str(path)
            )
        )

    return sql_objects

def get_object_type(reference: str):
    parts = reference.split('.')
    object_name = parts[2]
    if 'vw_' in object_name:
        return 'view'
    elif 'mv_' in object_name:
        return 'materialized view'
    elif 'proc_' in object_name:
        return 'procedure'
    elif '(' in object_name:
        return 'function'
    else:
        return 'schema'

class BrokenReferences:
    def __init__(self):
        self.refs = dict()

    def add_reference(self, reference, referencee):
        if reference not in self.refs:
            self.refs[reference] = list()

        self.refs[reference].append(referencee)

def get_broken_views(dataset: str, dataset_to_check: str, report_duplicates: Boolean = False) -> BrokenReferences:
    bq_path = get_bq_path()

    os.chdir(bq_path)

    all_files = paths_to_sql_objects(list(Path(bq_path).rglob('*.sql')))
    all_files.extend(paths_to_sql_objects(list(Path(bq_path).rglob('*/schema/*.json'))))
    dataset_files = list(filter((lambda f: f.dataset == dataset and f.client_name != 'bq'), all_files))
    
    refs = BrokenReferences()
    
    for file in dataset_files:

        client_root = bq_path + f"/{file.client_name}" if file.client_name != 'bq' else ''

        with open(file.file_path, 'r') as f:
            file.definition = f.read().replace('\n', ' ').replace('\t', '  ')


        words = file.definition.split(' ')
        object_references = list(filter(lambda w: '${project}' in w and 'temp.' not in w, words))
        for ref in object_references:
            de_tokenized = ref.replace('${dataset}', file.dataset).replace('`','').replace('${color}', 'blue')
            parts = de_tokenized.split('.')
            ref_type = get_object_type(de_tokenized)

            # These tables aren't typically checked in
            if ref_type == 'materialized view':
                continue

            object_name = re.sub(r'([a-zA-Z_0-9\*]+).*', r'\1', parts[2].split('(')[0])
            original_dataset = parts[1]
            referenced_file = SqlObject(
                object_name = object_name,
                object_type = ref_type,
                dataset = original_dataset,
                client_name = file.client_name,
                file_path = f"{client_root}/{parts[1]}/{ref_type}/{object_name}{'.json' if ref_type =='schema' else '.sql'}"
            )

            if referenced_file == file:
                continue

            matching_files = list(filter(lambda m: m == referenced_file, all_files))

            
            if len(matching_files) == 1:
                continue
            elif report_duplicates and len(matching_files) > 1:
                print_info(f"`{file.fully_qualified_name}`: Found more than one match for "+
                    f"`{referenced_file.fully_qualified_name}`", 0)
            elif len(matching_files) == 0:
                # print_info(f"`{file.fully_qualified_name}`: Found no file match for "+
                #     f"`{referenced_file.fully_qualified_name}`")
                referenced_file.dataset = ''
                matching_files = list(filter(lambda m: m == referenced_file, all_files))
                if len(matching_files) == 0:
                    # print_fail(f"`{file.fully_qualified_name}`: no file exists for "+
                    #     f"`{referenced_file.fully_qualified_name}`")
                    
                    referenced_file.dataset = original_dataset
                    refs.add_reference(referenced_file, file)

                if len(matching_files) == 1:
                    file.definition = file.definition.replace(
                        f"{referenced_file.dataset}.{referenced_file.object_name}",
                        f"{matching_files[0].dataset}.{referenced_file.object_name}"
                    )
                    print_info(f"`{file.fully_qualified_name}`: Reference to "+
                        f"`{referenced_file.fully_qualified_name}`"+
                        f", dataset changed ({original_dataset} -> {matching_files[0].dataset})")

                if len(matching_files) > 1:
                    print_fail(f"`{file.fully_qualified_name}`: Reference to "+
                        f"`{referenced_file.fully_qualified_name}`"+
                        f", has multiple candidates, resolve manually:")

                    for match in matching_files:
                        print(f"\t\t{match.fully_qualified_name}")

    return refs

def report_breaks(refs: BrokenReferences, referencers_to_display = 100):
    for reference, referencers in refs.refs.items():
        print_fail(
            message = f"No file found for {reference.client_name} `{reference.dataset}.{reference.object_name}, referencers:",
            indents = 0)
        ref_count = 0
        for referencer in referencers:
            if ref_count >= referencers_to_display:
                break

            print(f"\t{referencer.fully_qualified_name}")
            ref_count += 1

        if len(referencers) > 0:
            print('')

def main():
    dataset = 'ext'
    broken_refs = get_broken_views(dataset, 'core')
    report_breaks(broken_refs)

if __name__ == "__main__":
    main()