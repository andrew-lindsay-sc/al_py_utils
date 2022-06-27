from pathlib import Path
from helpers.StaticMethods import get_bq_path

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

    def __init__(
        self,
        fully_qualified_name: str
    ):
        self.fully_qualified_name = fully_qualified_name.replace('`','')
        name_parts = self.fully_qualified_name.split('.')
        self.bq_project = name_parts[0]
        self.dataset = name_parts[1]
        self.object_name = name_parts[2]

        self.object_type = self.get_object_type()

        # TODO: this needs to handle dev project names
        self.client_name = self.bq_project.split('-')[-1]
        client_root = get_bq_path()
        if not (self.object_name[-2:] == '_0' or self.dataset == 'core'):
            client_root += '/' + self.client_name

        self.file_path = \
            f"{client_root}/{self.dataset}/{self.object_type}/{self.object_name}"+ \
            f"{'.json' if self.object_type =='schema' else '.sql'}"

        self._definition = ''

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

    def get_object_type(self):
        # parts = self.object_name.split('.')
        # object_name = parts[2]
        if 'vw_' in self.object_name:
            return 'view'
        elif 'mv_' in self.object_name:
            return 'materialized view'
        elif 'proc_' in self.object_name:
            return 'procedure'
        elif '(' in self.object_name:
            return 'function'
        else:
            return 'schema'