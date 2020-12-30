from src.api_utilities import SongsBatchSource, UsersBatchSource
from src.cdc import ListeningSessionsCDC, SongsCDC
from src.destination import CloudDatalake, CloudStorage

def listening_sessions_job():
    datalake = CloudStorage(dir_path='data/listening_sessions')
    # datalake = CloudDatalake(dir_path='data/songs')
    ub = UsersBatchSource(users_file_path='src/users/users.json')
    lscdc = ListeningSessionsCDC(source=ub, 
                                 destination=datalake, 
                                 syncFile='data/listening_sessions/sync.json', 
                                 chrono_attr='threshold', 
                                 sync_attr='ts', 
                                 songs_file_path='data/songs_to_req')
    lscdc.get_fresh_rows()

def songs_cdc_job():
    sb = SongsBatchSource()
    datalake = CloudStorage(dir_path='data/songs')
    # datalake = CloudDatalake(dir_path='data/songs')
    scdc = SongsCDC(source=sb, 
                    destination=datalake, 
                    syncFile='data/songs/sync.json', 
                    songs_to_request_dir='data/songs_to_req', 
                    key_attr='song_id')
    scdc.get_fresh_rows()

if __name__ == "__main__":
    listening_sessions_job()