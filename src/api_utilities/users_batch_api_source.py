from .batch_api_source import BatchApiSource
import json

class UsersBatchSource(BatchApiSource):
    def __init__(self, file_path, sync_path):
        param_list = self.get_param_list(file_path, sync_path)
        super().__init__('user.getrecenttracks', param_list)

    def get_param_list(self, path, sync):
        tsh = 0
        with open(sync, 'r') as f:
            tsh = json.load(f)['threshold']

        with open(path, 'r') as f:
            data = json.load(f)
            return [{'method_params' : {'user' : e['name'], "from" : tsh}, 'other_params' : {'limit':500}} for e in data]

if __name__ == "__main__":
    b = UsersBatchSource('src/users/users.json', 'data/datalake_log/sync.json')
    table = b.read()
    from pprint import pprint
    pprint(table)
