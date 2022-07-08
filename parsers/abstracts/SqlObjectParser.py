from clients.BqClient import BqClient
from domain.SqlObject import SqlObject

import abc

class SqlObjectParser(metaclass=abc.ABCMeta):
    # Allow passing in a BqClient instance to avoid having to check dependencies again
    def __init__(self, sql_object: SqlObject, bq_instance: BqClient = None):
        self._sql_object = sql_object
        self._bq_instance = BqClient(self._sql_object.client_name) if bq_instance is None else bq_instance

    @property
    def object_type(self) -> str:
        return self._sql_object.object_type

    @property
    def dependencies(self) -> dict[str, list[SqlObject]]:
        if self._dependencies is None:
            self._dependencies = self._parse_dependencies()

        return self._dependencies

    def _parse_dependencies(self) -> dict[str, list[SqlObject]]:
        raise NotImplementedError("Child classes must implement this method")