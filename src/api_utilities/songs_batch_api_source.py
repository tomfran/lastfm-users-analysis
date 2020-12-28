from .batch_api_source import BatchApiSource
import json

class SongsBatchSource(BatchApiSource):
    def __init__(self):
        super().__init__('track.getinfo')

    def update_songs_to_request(self, songs_data):
        self.update_param_list(self.get_param_list(songs_data))

    def get_param_list(self, songs_data):
        return [{'method_params' : v, 'other_params' : {}} for k, v in songs_data.items()]