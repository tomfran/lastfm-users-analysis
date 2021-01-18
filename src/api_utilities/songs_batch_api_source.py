from .batch_api_source import BatchApiSource
import json

class SongsBatchSource(BatchApiSource):
    """
    Batch api source subclass to manage 
    multiple songs requests
    """
    def __init__(self):
        """
        Constructor
        """
        super().__init__('track.getinfo')

    def update_songs_to_request(self, songs_data):
        """
        Update the method param list

        Args:
            songs_data (list): list with all the song info 
            to request
        """
        self.update_param_list(self.get_param_list(songs_data))

    def get_param_list(self, songs_data):
        """
        Utility method to get the songs
        parameters in the right format

        Args:
            songs_data (list): songs to request title and artist

        Returns:
            list: list of dictionaries to send requests
        """
        return [{'method_params' : v, 'other_params' : {}} for k, v in songs_data.items()]