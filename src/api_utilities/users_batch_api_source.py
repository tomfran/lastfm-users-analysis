from .batch_api_source import BatchApiSource
import json

class UsersBatchSource(BatchApiSource):
    """
    Batch api source subclass to manage 
    multiple users recent tracks requests
    """
    def __init__(self, users_file_path):
        """
        Constructor

        Args:
            users_file_path (String): path to the user file
        """
        self.users_file_path = users_file_path
        super().__init__('user.getrecenttracks')

    def update_from(self, tsh):
        """
        Update the source method parameters

        Args:
            tsh (int): timestamp to request
        """
        self.update_param_list(self.get_param_list(tsh))
    
    def get_param_list(self, from_tsh):
        """
        Utility method to get the users
        parameters in the right format.
        It reads the user list from a file.

        Args:
            from_tsh (int): collect tracks listened after this timestamp

        Returns:
            list: list of dictionaries to send requests
        """
        with open(self.users_file_path, 'r') as f:
            data = json.load(f)
            return [{'method_params' : {'user' : e['name'], "from" : from_tsh}, 'other_params' : {'limit':500}} for e in data]
