from .batch_api_source import BatchApiSource
import json

class UsersBatchSource(BatchApiSource):
    def __init__(self, users_file_path):
        self.users_file_path = users_file_path
        super().__init__('user.getrecenttracks')

    def update_from(self, tsh):
        self.update_param_list(self.get_param_list(tsh))
    
    def get_param_list(self, from_tsh):
        with open(self.users_file_path, 'r') as f:
            data = json.load(f)
            return [{'method_params' : {'user' : e['name'], "from" : from_tsh}, 'other_params' : {'limit':5}} for e in data]
