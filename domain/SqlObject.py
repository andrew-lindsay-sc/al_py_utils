import json
from pathlib import Path
import re

from pyparsing import Generator
from helpers.StaticMethods import get_bq_path
from google.cloud.bigquery import SchemaField

class SqlObject:
    def __init__(
        self,
        fully_qualified_name: str,
        definition: str = None
    ):
        self.fully_qualified_name = fully_qualified_name.replace('`','')
        name_parts = self.fully_qualified_name.split('.')
        self.bq_project = name_parts[0]
        self.dataset = name_parts[1]
        self.object_name = name_parts[2]

        self.object_type = self._get_object_type()

        # TODO: this needs to handle dev project names
        self.client_name = self.bq_project.split('-')[-1]
        client_root = get_bq_path()
        if not (self.object_name[-2:] == '_0' or self.dataset == 'core'):
            client_root += '/' + self.client_name

        self.file_path = \
            f"{client_root}/{self.dataset}/{self.object_type}/{self.object_name}"+ \
            f"{'.json' if self.object_type =='schema' else '.sql'}"

        self._definition = '' if definition is None else definition

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
                
            self._definition = self._definition.replace("${project}", self.bq_project)\
                .replace("${dataset}", self.dataset)
            return True
        except Exception as error:
            raise Exception(f"Failed to read definition from file '{self.file_path}', error: {error}")

    def _set_definition(self, value: str):
        self._definition = value

    def _get_definition(self):
        if len(self._definition) == 0:
            self._init_definition()
            
        return self._definition

    definition = property(
        fget=_get_definition,
        fset=_set_definition,
        doc="The definition property."
    )

    def __hash__(self):
        return hash(self.fully_qualified_name)

    def _get_object_type(self):
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

    def get_schema_fields(self):
        if self.object_type != 'schema':
            raise Exception("This method is for schema/tables only.")

        for field in self.definition.split('},\n'):
            to_load = field if field[-1:] == '}' else field+'}'
            schema_json = json.loads(to_load)            
            yield SchemaField(
                name = schema_json['name'], 
                field_type = schema_json['type'], 
                mode = schema_json['mode']
            ) 