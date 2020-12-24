from abstract_classes import AbstractRegistryCDC

class SongsCDC(AbstractRegistryCDC):
    def access_fields(self, table):
        def process_track(tr):
            tr = tr['track']
            album = tr.get('album')
            artist = tr.get('artist')
            ret = {}
            # TODO check if every album has an image a title anb an artist
            if album:
                ret = {
                    'album_artist' : album['artist'],
                    'album_title'  : album['title'], 
                    'album_image'  : album['image'][3]['#text']
                }
            oth = {
                'artist_name' : artist['name'], 
                'title' : tr['name'], 
                'duration' : tr['duration'], 
                'url' : tr['url'],
                'song_id' : hash(artist['name'] + tr['name'])
            }
            ret.update(oth)
            return ret

        return [process_track(row) for row in table]

if __name__ == "__main__":
    from songs_batch_api_source import SongsBatchSource
    from cloud_datalake import CloudDatalake
    batch = SongsBatchSource('data/datalake_log/20201224.json')
    datalake = CloudDatalake('data/datalake_reg')
    cdc = SongsCDC(batch, datalake, 'data/datalake_reg/sync.json', 'song_id')
    cdc.get_fresh_rows()
    # cdc.source.method_params_list = [
    #         {   
    #             "method_params" : {'artist' : 'cher', 'track' : 'believe'}, 
    #             'other_params' : {}
    #         },
    #         {   
    #             "method_params" : {'artist' : 'Green Day', 'track' : '21 Guns'}, 
    #             'other_params' : {}
    #         }
    # ]
    # cdc.get_fresh_rows()
