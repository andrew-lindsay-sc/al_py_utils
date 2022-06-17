from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from PrintColors import *
from StaticMethods import print_warn

class BqClient:
    """Helper class to wrap bigQuery client initialization and operations"""
    def __init__(self, client_name):
        self.project_id = "soundcommerce-client-"+client_name
        self.client_name = client_name
        self.instance = bigquery.Client(project=self.project_id)

    all_tables_and_views_query = """
        SELECT concat(table_schema, '.', table_name) as full_name
        FROM region-us.INFORMATION_SCHEMA.VIEWS

        union all

        SELECT concat(table_schema, '.', table_name) as full_name
        FROM region-us.INFORMATION_SCHEMA.TABLES
    """

    def manage_object(self, operation, file):
        """
            (str, str) -> None
            Performs the specified BQ operation on the specified file.
        """
        file_parts = file.split('/')
        file_name = file_parts[-1]
        to_modify = bigquery.Table(self.project_id+'.'+file_name)
        dataset = file_parts[-3].strip()
        object_type = file_parts[-2]

        if operation == 'deleted' and object_type == 'table':
            print_warn(f"Table drops must be performed manually ({file_name}).")

        if object_type in ['table', 'view']:
            if operation == 'modified':
                with open(file, 'r') as f:
                    definition = f.read()

                to_modify.view_query = \
                    definition.replace("${project}", self.project_id)\
                        .replace("${dataset}", dataset)

                # Try to update the table/view
                try:
                    self.instance.get_table(to_modify)  # Make an API request.
                    self.instance.update_table(
                        table=to_modify, fields=["view_query"]
                    )
                # If it doesn't exist, create it
                except NotFound:
                    self.instance.create_table(
                        table=to_modify
                    )
            elif operation == 'deleted' and object_type != 'table': #object_type check is redundant, just being explicit
                # Delete the view, if it doesn't exist, oh well, mission accomplished anyway
                self.instance.delete_table(table = to_modify, not_found_ok=True)
        else:
            raise NotImplementedError("Only tables and views are supported at this time")

        return f"({object_type}) {dataset}.{file_name} has been {operation}"
    
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
        query = query.replace('[replace_me]', ',\n\t'.join(objects))
        return self.instance.query(query).result()