from abc import ABCMeta, abstractmethod

class AbstractCDC(metaclass = ABCMeta):

    def __init__(self, source, destination, syncFile):
        self.source = source
        self.destination = destination
        self.syncFile = syncFile

    @abstractmethod
    def get_fresh_rows(self):
        pass
    
    @abstractmethod
    def update_sync(self):
        pass