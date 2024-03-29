from src.api_utilities import SongsBatchSource, UsersBatchSource
from src.cdc import ListeningSessionsCDC, SongsCDC
from src.destination import CloudDatalake, CloudStorage
import shutil
import os

def clean_data():
    """
    Remove local data folder
    """
    try:
        shutil.rmtree('data')
    except:
        pass
        
def listening_sessions_job():
    """
    Start the listening sessions job.
    This will create source destination and cdc
    class instances to then run the get fresh rows method
    """
    datalake = CloudStorage(dir_path='data/listening_sessions')
    ub = UsersBatchSource(users_file_path='src/users/users.json')
    lscdc = ListeningSessionsCDC(source=ub, 
                                 destination=datalake, 
                                 syncFile='data/listening_sessions/sync.json', 
                                 chrono_attr='threshold', 
                                 sync_attr='ts', 
                                 songs_file_path='data/songs_to_req')
    lscdc.get_fresh_rows()

def songs_cdc_job():
    """
    Start the songs sessions job.
    This will create source destination and cdc
    class instances to then run the get fresh rows method
    """
    sb = SongsBatchSource()
    datalake = CloudStorage(dir_path='data/songs')
    scdc = SongsCDC(source=sb, 
                    destination=datalake, 
                    syncFile='data/songs/sync.json', 
                    songs_to_request_dir='data/songs_to_req', 
                    key_attr='song_id')
    scdc.get_fresh_rows()

def shutdown_vm():
    """
    Shutdown the VM after cdc job finishes
    """
    os.system('sudo shutdown now')

if __name__ == "__main__":
    try:
        clean_data()
        listening_sessions_job()
        songs_cdc_job()
    except:
        pass
    shutdown_vm()