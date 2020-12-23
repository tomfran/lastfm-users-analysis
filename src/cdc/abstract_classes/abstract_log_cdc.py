import json
from os import path
import time
from abc import ABCMeta, abstractmethod
from .abstract_cdc import AbstractCDC

class AbstractLogCDC(AbstractCDC, metaclass = ABCMeta):

    def __init__(self, source, destination, syncFile, chrono_attr, sync_attr):
        super().__init__(source, destination, syncFile)
        self.chrono_attr = chrono_attr
        self.sync_attr = sync_attr

    def get_fresh_rows(self):
        self.destination.rollback()
        # print('\tLOG CDC: looking for new tuples')
        table = self.access_fields(self.source.read())
        ths = self.read_from_sync()
        # print('\tLOG CDC: comparing chrono attribute with old max one')
        filtered = [row for row in table if row[self.sync_attr] > ths]
        if filtered:
            # print(f'\tLOG CDC: found {len(filtered)} new lines')
            self.destination.write(filtered)

            new_ths = max([x[self.sync_attr] for x in filtered])

            self.update_sync(new_ths)
            self.destination.commit()
            # print('\tLOG CDC: done')
        # else:
            # print('\tLOG CDC: nothing changed\n')

    def read_from_sync(self):
        if not path.isfile(self.syncFile):
            with open(self.syncFile, 'w') as f:
                dd = { self.chrono_attr : -1 }
                json.dump(dd, f)
        
        with open(self.syncFile, 'r') as f:
            return json.load(f).get(self.chrono_attr)

    def update_sync(self, ths):
        # print('\tLOG CDC: updatind the sync file')
        with open(self.syncFile, 'w') as f:
            dd = { self.chrono_attr : ths }
            json.dump(dd, f)

    @abstractmethod
    def access_fields(self, table):
        pass