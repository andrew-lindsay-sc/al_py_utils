import abc

class DevToolsModule(metaclass=abc.ABCMeta):
    def __init__(self):
        self._arguments = None

    @abc.abstractmethod
    def execute(self) -> bool:
        pass