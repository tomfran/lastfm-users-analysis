from .abstract_classes import AbstractRegistryCDC
from datetime import datetime
import json

class SongsCDC(AbstractRegistryCDC):
    """
    Songs cdc implementation
    It implements a registry cdc logic, as songs can change over
    time, and no chrono attribute can be used.
    
    """
    def __init__(self, source, destination, syncFile, songs_to_request_dir, key_attr):
        """
        Constructor

        Args:
            source (AbstractSource): source class to read data
            destination (AbstractDestination): class that implements write logic
            syncFile (String): path to the sync file to update 
            songs_to_request_dir (String): path to the directory containing 
                                           songs to request file
            key_attr (String): attribute to be used as a key in the registry
                               cdc logic
        """
        super().__init__(source, destination, syncFile, key_attr)
        # update the songs source param list, to get the songs 
        # according to the sync file
        self.songs_to_request_file = f"{songs_to_request_dir}/{datetime.today().strftime('%Y%m%d')}.json"
        self.update_source()
    
    def update_source(self):
        """
        Get songs to request from the file in the songs to request path, 
        and update source parameters
        """
        # read from songs to request file, that is, the songs listened by users today
        with open(self.songs_to_request_file, 'r') as f:
            data = json.load(f)
        # source is of type 'SongsBatchSource'
        self.source.update_songs_to_request(data)

    def access_fields(self, table):
        """
        Access the needed fields from the source data

        Args:
            tables (list): list of data coming from the source

        Returns:
            list: list of properly trimmed data
        """
        def process_track(tr):
            tr = tr['track']
            artist = tr.get('artist')
            # TODO check if every album has an image a title anb an artist
            ret = {
                'artist_name' : artist['name'], 
                'title' : tr['name'], 
                'duration' : tr['duration'], 
                'url' : tr['url'],
                'song_id' : hash(tr['url'].lower())
            }

            album = tr.get('album')
            if album:
                ret.update({
                    'album_artist' : album['artist'],
                    'album_title'  : album['title'], 
                    'album_image'  : album['image'][3]['#text']
                })
            
            toptags = tr.get('toptags')
            if toptags:
                if toptags.get('tag'):
                    ret['top_tags'] = tuple([d['name'] for d in toptags['tag']])        
            return ret

        return [process_track(row) for row in table]
