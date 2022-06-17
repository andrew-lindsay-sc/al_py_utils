import abc

class FileParser(metaclass=abc.ABCMeta):
    def __init__(self):
        self.changed_files = self.parse_changed_files()
        self._files_by_client = self.parse_clients()

    @property
    def files_by_client(self):
        self._files_by_client = self.parse_clients()
        return self._files_by_client

    @abc.abstractmethod
    def parse_clients(self):
        pass

    # @abc.abstractmethod
    # def print_file_info(self):
    #     pass

    # @abc.abstractmethod
    # def report_files(self):
    #     pass