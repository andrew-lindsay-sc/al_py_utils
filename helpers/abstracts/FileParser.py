import abc

class FileParser(metaclass=abc.ABCMeta):
    def __init__(self):
        self.changed_files = self.parse_changed_files()
        self.files_by_client = self.parse_clients()

    @abc.abstractmethod
    def parse_changed_files(self):
        pass

    @abc.abstractmethod
    def parse_clients(self):
        pass