from abstract_log_cdc import AbstractLogCDC

class ListeningSessionsCDC(AbstractLogCDC):
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
    from cloud_datalake import CloudDatalake
    source = ApiSource(method='user.getrecenttracks', method_params={'user' : 'giacomo109', 'from' : '1608398626'}, other_params={'limit':500})
    datalake = CloudDatalake('data/datalake_log')
    cdc = ListeningSessionsCDC(source, datalake, 'data/datalake_log/sync.json', 'threshold', 'ts')
    cdc.get_fresh_rows()