from .abstract_classes import AbstractLogCDC
import time
from datetime import datetime
import json
import os

class ListeningSessionsCDC(AbstractLogCDC):
    """
    Listening sessions cdc implementation
    It implements a log cdc logic, as listening sessions have a 
    timestamp as chrono attribute.
    """
    def __init__(self, source, destination, syncFile, chrono_attr, sync_attr, songs_file_path):
        """
        Constructor

        Args:
            source (AbstractSource): source class to read data
            destination (AbstractDestination): class that implements write logic
            syncFile (str): path to the sync file to update 
            chrono_attr (str): chrono attribute in the data
            sync_attr (str): sync attribute to implement log cdc logic
            songs_file_path (str): path to songs to request file
        """
        super().__init__(source, destination, syncFile, chrono_attr, sync_attr)
        self.songs_file_path = songs_file_path
        if not os.path.isdir(songs_file_path):
            os.makedirs(songs_file_path)

    def get_fresh_rows(self):
        """
        Get the fresh rows from the source.
        It implements the log cdc logic reading the chrono attribute
        from the sync file.
        """
        self.destination.rollback()
        # read the threshold from the sync file
        ths = self.read_from_sync()
        # update 'from' parameter in the batch recent tracks source, 
        # to get only new listened songs
        self.source.update_from(ths)
        table = self.access_fields(self.source.read())
        # no need to filter only tracks newer then ths, as the api request
        # already do so
        if table:
            self.destination.write(table)
            new_ths = max([x[self.sync_attr] for x in table])
            self.update_sync(new_ths)
            self.destination.commit()

    def save_songs_to_request(self, songs_dict):
        """
        Save songs to request.
        Basically a list of all the songs listenend by the users.

        Args:
            songs_dict (dict): dictionary with 'song_id' and song details
        """
        path = f"{self.songs_file_path}/{datetime.today().strftime('%Y%m%d')}.json"
        with open(path, 'a+') as f:
            f.write(json.dumps(songs_dict, indent=4, sort_keys=True, ensure_ascii=False))
        self.destination.upload_songs_to_request(path)

    def access_fields(self, tables):
        """
        Access the needed fields from the source data

        Args:
            tables (list): list of data coming from the source

        Returns:
            list: list of properly trimmed data
        """
        songs_dict = {}
        def process_track(tr, user):
            try:
                if tr.get('@attr') and tr['@attr']['nowplaying'] == 'true':
                    return {}

                artist = tr['artist']['#text']
                name = tr['name']
                key = hash(tr['url'].lower())
                songs_dict[key] = {"artist": artist, "track" : name}
                return {
                    'user_id' : user,
                    'song_id' : key,
                    'ts' : int(tr['date']['uts'])
                }
            except:
                # if not os.path.isdir("log"):
                #     os.makedirs("log")
                # with open("log/listening_session_errors.log", 'a+') as f:
                #     f.write(f"{datetime.today().strftime('%Y%m%d')}-{str(tr)}\n")
                # TODO: sistemare logging
                return {}
        ret = []
        for table in tables:
            user = table['recenttracks']['@attr']['user']
            ret += [process_track(e, user=user) for e in table['recenttracks']['track']]
        self.save_songs_to_request(songs_dict)
        return [e for e in ret if e]