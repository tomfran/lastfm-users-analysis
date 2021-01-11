from .abstract_cdc import AbstractCDC
import json
from os import path
from abc import ABCMeta, abstractmethod

class AbstractRegistryCDC(AbstractCDC, metaclass = ABCMeta):

    def __init__(self, source, destination, syncFile, key_attr):
        super().__init__(source, destination, syncFile)
        self.key_attr = key_attr

    def get_fresh_rows(self):
        self.destination.rollback()
        # print('\tREGISTRY CDC: looking for new tuples')
        table = self.access_fields(self.source.read())
        # print('\tREGISTRY CDC: computing hashes and comparing with old state')
        state = [{
                    'khash' : hash(e[self.key_attr]), 
                    'hash'  : hash(tuple(v for k, v in e.items() if k != self.key_attr))
                } for e in table ]
        old_state = self.read_from_sync()

        # find inserted rows
        current_keys_set = set(e['khash'] for e in state)
        old_keys_set = set(e['khash'] for e in old_state)
        inserted_keys = current_keys_set - old_keys_set
        
        inserted_rows = []
        if inserted_keys:
            inserted_rows = [e for e in table if hash(e[self.key_attr]) in inserted_keys]
            # print(f'\tREGISTRY CDC: found {len(inserted_rows)} new lines')

        # find modified rows
        modified_keys = []
        for e1 in state:
            for e2 in old_state:
                if e1['khash'] == e2['khash'] and e1['hash'] != e2['hash']:
                    modified_keys.append(e1['khash'])

        modified_rows = []
        if modified_keys:
            modified_rows = [e for e in table if hash(e[self.key_attr]) in modified_keys]
            # print(f'\tREGISTRTY CDC: found {len(modified_rows)} modified lines')
        
        new_state = state
        new_state += [e for e in old_state if e['khash'] not in current_keys_set]

        if inserted_rows + modified_rows:
            self.destination.write(inserted_rows + modified_rows)
            self.update_sync(new_state)
            self.destination.commit()
            # print('\tREGISTRY CDC: done')
        # else:
            # print('\tREGISTRY CDC: nothing changed\n')


    def read_from_sync(self):
        if not path.isfile(self.syncFile):
            with open(self.syncFile, 'w') as f:
                json.dump({}, f)
        with open(self.syncFile, 'r') as f:
            return json.load(f)

    def update_sync(self, state):
        # print('\tREGISTRY CDC: updating the sync file')
        with open(self.syncFile, 'w') as f:
            f.write(json.dumps(state, indent=4, sort_keys=True))

    @abstractmethod
    def access_fields(self, table):
        pass