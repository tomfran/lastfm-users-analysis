from .abstract_classes import AbstractRegistryCDC
from datetime import datetime
import json

class SongsCDC(AbstractRegistryCDC):
    def __init__(self, source, destination, syncFile, songs_to_request_dir, key_attr):
        super().__init__(source, destination, syncFile, key_attr)
        # update the songs source param list, to get the songs 
        # according to the sync file
        self.songs_to_request_file = f"{songs_to_request_dir}/{datetime.today().strftime('%Y%m%d')}.json"
        self.update_source()
    
    def update_source(self):
        # read from songs to request file, that is, the songs listened by users today
        with open(self.songs_to_request_file, 'r') as f:
            data = json.load(f)
        # source is of type 'SongsBatchSource'
        self.source.update_songs_to_request(data)

    def access_fields(self, table):
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
