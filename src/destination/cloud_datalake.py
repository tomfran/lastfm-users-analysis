from .abstract_classes import AbstractDestination
import os
from datetime import datetime
import json

class CloudDatalake (AbstractDestination):
    def __init__(self, dir_path):
        self.dir_path = dir_path
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)
        
    # TODO mettere data oggi, per GCP fare classe figlio che riscrive write
	# writes rows in files .tmp
    def write(self, rows):
        # print("\tDATALAKE: Writing tmp rows")
        # to_write = []
        with open(f"{self.dir_path}/update_{datetime.today().strftime('%Y%m%d')}.tmp", 'a') as f:
            # for row in rows:
            #     dd = {'timestamp' : str(datetime.now())}
            #     dd.update(row)
            #     to_write.append(dd)
            # f.write(json.dumps(to_write, indent=4, sort_keys=True))
            f.write(json.dumps(rows, indent=4, sort_keys=True, ensure_ascii=False))
	# commits tmp files to json files
    def commit(self):
        # print("\tDATALAKE: Committing tmp to json")
        file_names = [e for e in os.listdir(self.dir_path) if '.tmp' in e]
        for f in file_names:
            os.rename(f'{self.dir_path}/{f}', f"{self.dir_path}/{f.replace('.tmp','.json')}")
            # TODO controllare se riscrive file o cambia effettivamente solo il nome
	# checking for existence of tmp files and removes them
    def rollback(self):
        # print("\tDATALAKE: Checking if tmp file are on datalake")
        file_names = [e for e in os.listdir(self.dir_path) if '.tmp' in e]
        # if file_names:
            # print("\tDATALAKE: Removing inconsistent data")
        for f in file_names:
            os.remove(f'{self.dir_path}/{f}')

		
