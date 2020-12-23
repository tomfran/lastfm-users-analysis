from abstract_classes import AbstractRegistryCDC

class SongsCDC(AbstractRegistryCDC):
    def __process_track(self, tr, user):
        return {
            'user_id' : user,
            'song_id' : hash(tr['artist']['#text'] + tr['name']),
            'ts' : int(tr['date']['uts'])
        }
    
    def access_fields(self, table):
        user = table['recenttracks']['@attr']['user']
        return [self.__process_track(e, user=user) for e in table['recenttracks']['track']]


if __name__ == "__main__":
    from api_source import ApiSource
    from pprint import pprint
    # from cloud_datalake import CloudDatalake
    a = ApiSource(method='track.getInfo', method_params={'artist' : 'cher', 'track' : 'believe'}, other_params={})
    pprint(a.read())
    # source = ApiSource(method='user.getrecenttracks', method_params={'user' : 'giacomo109', 'from' : '1608398626'}, other_params={'limit':500})
    # datalake = CloudDatalake('data/datalake_log')
    # cdc = SongsCDC(source, datalake, 'data/datalake_log/sync.json', 'threshold', 'ts')
    # cdc.get_fresh_rows()