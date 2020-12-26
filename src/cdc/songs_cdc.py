from .abstract_classes import AbstractRegistryCDC

class SongsCDC(AbstractRegistryCDC):
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
                'song_id' : hash(artist['name'] + tr['name']),
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

if __name__ == "__main__":
    from songs_batch_api_source import SongsBatchSource
    from cloud_datalake import CloudDatalake
    batch = SongsBatchSource('data/datalake_log/20201226.json')
    
    datalake = CloudDatalake('data/datalake_reg')
    cdc = SongsCDC(batch, datalake, 'data/datalake_reg/sync.json', 'song_id')
    cdc.get_fresh_rows()
