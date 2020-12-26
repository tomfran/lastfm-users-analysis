from .batch_api_source import BatchApiSource
import json

class SongsBatchSource(BatchApiSource):
    def __init__(self, file_path):
        param_list = self.get_param_list(file_path)
        super().__init__('track.getinfo', param_list)

    def get_param_list(self, path):
        with open(path, 'r') as f:
            data = json.load(f)
            return [{'method_params' : v, 'other_params' : {}} for k, v in data.items()]

if __name__ == "__main__":
    b = SongsBatchSource('data/datalake_log/20201224.json')