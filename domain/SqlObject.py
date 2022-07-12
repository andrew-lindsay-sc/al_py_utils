import json
from pathlib import Path
import re
from google.cloud.bigquery import RoutineType
from google.cloud import bigquery, bigquery_v2

from pyparsing import Generator
from helpers.StaticMethods import get_bq_path
from google.cloud.bigquery import SchemaField

class SqlObject:
    def __init__(
        self,
        fully_qualified_name: str,
        definition: str = None
    ):
        self.return_type = None
        self.args = list()

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

    def _parse_args(self):
        re_match = re.match(r'CREATE.*`.*`\((.*)\)', self._definition)
        if len(re_match.group(1)) > 0:
            arguments_text = [x.strip() for x in re_match.group(1).split(',')]
            if len(arguments_text) > 0:
                for arg in arguments_text:
                    parts = arg.split(' ')
                    self.args.append(
                        bigquery.RoutineArgument(
                            name = parts[0].strip(),
                            data_type = bigquery_v2.types.StandardSqlDataType(
                                type_kind = parts[1].strip()
                            ),
                        )
                    )

    def _init_definition(self):
        try:
            with open(self.file_path, 'r') as f:
                self._definition = f.read()                    
                
            self._definition = self._definition.replace("${project}", self.bq_project)\
                .replace("${dataset}", self.dataset)

            self.routine_type = self._get_routine_type()
            
            # Strip "CREATE" statement as that angers the google
            if self._definition[0:6] == 'CREATE':
                if self.object_type in ['function', 'procedure']:
                    self._parse_args()
                    if self.routine_type == RoutineType.SCALAR_FUNCTION:
                        self._definition = re.sub(r'CREATE.+`.+`\(.*\)(.*)', r'\1', self._definition)
                        re_search = re.search('RETURNS[ \t]+([a-zA-Z0-9]+)(?s:.*?).*AS', self._definition, re.M)
                        self.return_type = bigquery_v2.types.StandardSqlDataType(
                                type_kind = re_search.group(1)
                            )
                        self._definition = self.definition[re_search.regs[0][-1]:]
                        if self._definition.strip()[-1] == ';':
                            self._definition = self._definition.strip()[:-1]
                    else:
                        self._definition = re.sub(r'CREATE.+`.+`\(.*\)\((.*)', r'\1', self._definition)
                        self._definition = self._definition.strip()[:-1]

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
        elif '(' in self.object_name or 'fn_' in self.object_name:
            return 'function'
        else:
            return 'schema'

    def _get_routine_type(self):
        if self.object_type == 'procedure':
            return RoutineType.PROCEDURE
        elif self.object_type == 'function':
            if 'RETURNS' in self.definition:
                return RoutineType.SCALAR_FUNCTION
            else:
                return RoutineType.TABLE_VALUED_FUNCTION
        else:
            return RoutineType.ROUTINE_TYPE_UNSPECIFIED

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