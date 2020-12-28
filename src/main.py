from api_utilities import SongsBatchSource, UsersBatchSource
from cdc import ListeningSessionsCDC, SongsCDC
from destination import CloudDatalake
from datetime import datetime

def main():
    datalake = CloudDatalake(dir_path='data/listening_sessions')
    ub = UsersBatchSource(users_file_path='src/users/users_small.json')
    lscdc = ListeningSessionsCDC(source=ub, 
                                 destination=datalake, 
                                 syncFile='data/listening_sessions/sync.json', 
                                 chrono_attr='threshold', 
                                 sync_attr='ts', 
                                 songs_file_path='data/songs_to_req')
    lscdc.get_fresh_rows()

    sb = SongsBatchSource()
    datalake = CloudDatalake(dir_path='data/songs')
    scdc = SongsCDC(source=sb, 
                    destination=datalake, 
                    syncFile='data/songs/sync.json', 
                    songs_to_request_dir='data/songs_to_req', 
                    key_attr='song_id')
    scdc.get_fresh_rows()

if __name__ == "__main__":
    main()