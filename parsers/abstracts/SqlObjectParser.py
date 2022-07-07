import abc
from clients.BqClient import BqClient
from domain.SqlObject import SqlObject

class SqlObjectParser(metaclass=abc.ABCMeta):
    def __init__(self, sql_object: SqlObject):
        self._sql_object = sql_object
        self._bq_instance = BqClient(self._sql_object.client_name)

    @property
    def object_type(self) -> str:
        return self._sql_object.object_type

    @property
    def dependencies(self) -> dict[str, list[SqlObject]]:
        if self._dependencies is None:
            self._parse_dependencies = self._parse_dependencies

        return self._dependencies

    def _parse_dependencies(self) -> None:
        raise NotImplementedError("Child classes must implement this method")