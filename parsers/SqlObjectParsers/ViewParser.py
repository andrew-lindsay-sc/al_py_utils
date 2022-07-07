from domain.SqlObject import SqlObject
from parsers.abstracts.SqlObjectParser import SqlObjectParser

class ViewParser(SqlObjectParser):
    def __init__(self, sql_object: SqlObject):
        super().__init__(sql_object)

    def _get_dependent_views_sql(self):
        all_views_query = """
            SELECT concat(table_schema, '.', table_name) as full_name, view_definition
            FROM region-us.INFORMATION_SCHEMA.VIEWS
        """
        all_views = self._bq_instance.query(all_views_query).result()

        for view in all_views:
            vvw_matches = filter(lambda vvw: f"{vvw.dataset_id}.{vvw.table_id}" in view.view_definition, versioned_views)
            for vvw_match in vvw_matches:
                key = f"{vvw_match.dataset_id}.{vvw_match.table_id}"
                if key in dependencies:
                    dependencies[key].append(view.full_name)
                else:
                    dependencies[key] = [view.full_name]

    def _parse_dependencies(self) -> None:
        pass
