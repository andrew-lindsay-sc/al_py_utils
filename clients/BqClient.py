import re
from google.cloud import bigquery
from google.cloud import bigquery_v2
from google.cloud.exceptions import NotFound
from domain.SqlObject import SqlObject
from helpers.PrintColors import *
from helpers.StaticMethods import *
from enum import Enum

class BqClient:
    class Operation(Enum):
        MODIFIED = 1
        DELETED = 2

    """Helper class to wrap bigQuery client initialization and operations"""
    def __init__(self, client_name, project_id = None, skip_instance = False):
        if project_id is None:
            project_id = client_name if 'sandbox' in client_name else "soundcommerce-client-"+client_name
        self.project_id = project_id
        self.client_name = client_name
        self.instance = None
        if not skip_instance:
            self.instance = bigquery.Client(project=self.project_id)

    def __del__(self):
        if self.instance:
            self.instance._http.close()

    all_tables_and_views_query = """
        with data as (
            SELECT concat(table_schema, '.', table_name) as full_name, table_schema
            FROM region-us.INFORMATION_SCHEMA.VIEWS

            union all

            SELECT concat(table_schema, '.', table_name) as full_name, table_schema
            FROM region-us.INFORMATION_SCHEMA.TABLES
        )
        -- unsure why but it seems we sometimes get duplicates in here
        select distinct full_name from data
    """

    def _fetch_object_definitions(self) -> dict[str, list]:
        all_objects_query = """
            SELECT 'view' as object_type, concat(table_schema, '.', table_name) as full_name, view_definition as definition
            FROM region-us.INFORMATION_SCHEMA.VIEWS

            union all

            SELECT routine_type as object_type, concat(routine_schema, '.', routine_name) as full_name, routine_definition as definition
            FROM region-us.INFORMATION_SCHEMA.ROUTINES
            where routine_type != 'PROCEDURE'
        """
        results = self.instance.query(all_objects_query).result()
        object_definitions = dict[str, list()]
        for result in results:
            if result.object_type not in object_definitions:
                object_definitions[result.object_type] = list()
            
            object_definitions[result.object_type].append(
                SqlObject(
                    fully_qualified_name = f"`{self.project_id}.{result.full_name}`",
                    definition = result.definition
                )
            )

        return object_definitions

    @property
    def object_definitions(self) -> dict[str, str]:
        if self._object_definitions is None:
            self._object_definitions = self._fetch_object_definitions()

        return self._object_definitions

    def _create_dataset_if_not_exists(self, dataset: str) -> None:
        fully_qualified_dataset = f"{self.project_id}.{dataset}"
        try:
            self.instance.get_dataset(bigquery.Dataset(fully_qualified_dataset))
        except NotFound:
            self.instance.create_dataset(fully_qualified_dataset)

    def _manage_view(self, operation, sql_object: SqlObject):
        to_modify = bigquery.Table(sql_object.fully_qualified_name)
        to_modify.view_query = sql_object.definition

        if operation == self.Operation.MODIFIED:
            self._create_dataset_if_not_exists(sql_object.dataset)

            # Try to update the table/view
            try:
                to_modify = self.instance.get_table(to_modify)
                if to_modify.view_query == sql_object.definition:
                    return f"Skipping {sql_object.dataset}.{sql_object.object_name}, definition is already up to date."

                to_modify.view_query = sql_object.definition
                self.instance.update_table(
                    table=to_modify, fields=["view_query"]
                )
            # If it doesn't exist, create it
            except NotFound:
                self.instance.create_table(
                    table=to_modify
                )                
                return f"({sql_object.object_type}) {sql_object.dataset}.{sql_object.object_name} has been created."

        elif operation == self.Operation.DELETED:
            try:
                self.instance.delete_table(table = to_modify)
            except NotFound:
                return f"{sql_object.object_name} does not exist and will be skipped."

    def _manage_table(self, operation: Operation, sql_object: SqlObject):
        to_modify = bigquery.Table(sql_object.fully_qualified_name)
        to_modify.schema = list(sql_object.get_schema_fields())

        if operation == self.Operation.MODIFIED:
            self._create_dataset_if_not_exists(sql_object.dataset)

            # Try to update the table/view
            try:
                to_modify = self.instance.get_table(to_modify)
                existing_columns = [x.name for x in to_modify.schema]
                definition_columns = list(sql_object.get_schema_fields())
                columns_in_file = [x.name for x in definition_columns]

                missing_columns = [x for x in existing_columns if x not in columns_in_file]
                if len(missing_columns) > 0:
                    missing_string = ', '.join()
                    print_warn(f"The following columns are missing from the provided definition: {missing_string}")
                    print_warn(f"They will not be dropped.")

                columns_to_add = [x for x in columns_in_file if x not in existing_columns]
                if len(columns_to_add) == 0:
                    return f"Table schema already matches for {to_modify.full_table_id}, skipping."
                else:
                    new_schema = to_modify.schema[:]  # Creates a copy of the schema.
                    for col in definition_columns:
                        if col.name in columns_to_add:
                            new_schema.append(col)

                    to_modify.schema = new_schema
                    self.instance.update_table(table=to_modify, fields=["schema"])
                    return f"Table schema has been updated for {to_modify.full_table_id}."

            # If it doesn't exist, create it
            except NotFound:
                self.instance.create_table(
                    table=to_modify
                )                
                return f"({sql_object.object_type}) {sql_object.dataset}.{sql_object.object_name} has been created."

        elif operation == self.Operation.DELETED:
            return f"This utility will not drop tables; no operation has been performed on {sql_object.dataset}.{sql_object.object_name}."

    def _manage_function(self, operation, sql_object: SqlObject):
        # TODO: Handle arguments, see https://cloud.google.com/bigquery/docs/samples/bigquery-create-routine#bigquery_create_routine-python
        if operation == self.Operation.MODIFIED:
            to_modify = bigquery.Routine(
                routine_ref = sql_object.fully_qualified_name,
                body = sql_object.definition,
                type_ = sql_object.routine_type,
                arguments = sql_object.args,
                return_type = sql_object.return_type
            )
            self._create_dataset_if_not_exists(sql_object.dataset)
            # Try to update the table/view
            try:
                to_modify = self.instance.get_routine(to_modify)
                if to_modify.body == sql_object.definition:
                    return f"Skipping {sql_object.dataset}.{sql_object.object_name}, definition is already up to date."

                to_modify.body = sql_object.definition
                self.instance.update_routine( # For some reason you need to specify type_ or the API bombs
                    routine=to_modify, fields=["body", "type_", "return_type"]
                )
            # If it doesn't exist, create it
            except NotFound:
                self.instance.create_routine(
                    routine=to_modify
                )                
                return f"({sql_object.object_type}) {sql_object.dataset}.{sql_object.object_name} has been created."

        elif operation == self.Operation.DELETED:
            try:
                self.instance.delete_routine(
                    bigquery.Routine(routine_ref = sql_object.fully_qualified_name)
                )
            except NotFound:
                return f"{sql_object.object_name} does not exist and will be skipped."

    def _manage_proc(self, operation, sql_object: SqlObject):
        # Currently no divergence in how we handle functions and procs, this may change in the future though
        self._manage_function(operation, sql_object)

    # TODO: Refactor this to use SqlObject
    def manage_object(self, operation: Operation, sql_object: SqlObject) -> str:
        """
            (str, str) -> None
            Performs the specified BQ operation on the specified file.
        """

        if sql_object.object_type in ['table', 'schema']:
            return self._manage_table(operation, sql_object)
        elif sql_object.object_type == 'view':
            return self._manage_view(operation, sql_object)
        # TODO: Implement Procs and Function
        elif sql_object.object_type == 'function':
            return self._manage_function(operation, sql_object)
        elif sql_object.object_type == 'procedure':
            return  self._manage_proc(operation, sql_object)
        else:
            raise NotImplementedError("Only tables and views are supported at this time")
    
    def check_objects_exist(self, objects):
        """
            (list<str>) -> list<str>
            Returns a list of the objects which were provided as an argument and exist in BQ.
        """
        results = self.instance.query(self.all_tables_and_views_query).result()
        views_and_tables = list()
        for result in results:
            views_and_tables.append(result.full_name)

        return list(filter(lambda o: o in views_and_tables, objects))
    
    def fetch_definitions(self, objects):
        """
            (list<str>) -> list<Row>
            Returns a list of the objects which were provided as an argument and exist in BQ.
        """
        # TODO: Implement table schema verification, implement procs/functions
        query = """
            SELECT concat(table_schema, '.', table_name) as full_name, view_definition
            FROM region-us.INFORMATION_SCHEMA.VIEWS
            WHERE concat(table_schema, '.', table_name) in (
                [replace_me]
            )

        """
        quoted_objects = list()
        for obj in objects:
            quoted_objects.append(f"'{obj}'" if not is_quoted(obj) else obj)

        query = query.replace('[replace_me]', ',\n\t'.join(quoted_objects))
        return self.instance.query(query).result()

    def get_views_and_tables(self, datasets: list[str] = []):
        """
            (optional list[str]) -> list[str]
            Returns all views and tables currently present in BQ for the associated client
        """
        query = BqClient.all_tables_and_views_query
        if len(datasets) > 0:
            query += '\nWHERE table_schema in (\n\t'
            quoted_datasets = list()
            for dataset in datasets:
                quoted_datasets.append(f"'{dataset}'")
            query += (",\n\t".join(quoted_datasets))
            query += '\n)'

        results = self.instance.query(query).result()
        views_and_tables = list()
        for result in results:
            views_and_tables.append(result.full_name)

        return views_and_tables

    def path_to_fully_qualified(self, path: str) -> str:
        return self.path_to_fully_qualified(Path(path))

    def path_to_fully_qualified(self, path: Path) -> str:
        """
            (os.Path) -> str
            Turn a system path into a fully qualified SQL object name.
            This needs to exist here so that we have awareness of the associated project-id.
        """
        object_name = path[-1].replace('.sql','')
        dataset = path[-3]
        fully_qualified_name = f"`{self.project_id}.{dataset}.{object_name}`"
        return fully_qualified_name