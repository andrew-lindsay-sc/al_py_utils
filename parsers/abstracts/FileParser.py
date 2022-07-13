import abc

class FileParser(metaclass=abc.ABCMeta):
    def __init__(self):
        self.changed_files = self._parse_changed_files()
        self._files_by_client = self.parse_clients()

    @property
    def files_by_client(self):
        if self._files_by_client is None:
            self._files_by_client = self.parse_clients()

        return self._files_by_client

    @abc.abstractmethod
    def parse_clients(self):
        pass

    @abc.abstractmethod
    def _parse_changed_files(self):
        pass