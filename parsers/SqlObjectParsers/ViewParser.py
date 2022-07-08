from domain.SqlObject import SqlObject
from parsers.abstracts.SqlObjectParser import SqlObjectParser

# This class may not be needed, evaluating
class ViewParser(SqlObjectParser):
    def __init__(self, sql_object: SqlObject):
        super().__init__(sql_object)

    def _parse_dependencies(self) -> dict[str, list[SqlObject]]:
        object_db_version = self._bq_instance.object_definitions['view'].find(self._sql_object)
        return
