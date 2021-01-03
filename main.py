from src.api_utilities import SongsBatchSource, UsersBatchSource
from src.cdc import ListeningSessionsCDC, SongsCDC
from src.destination import CloudDatalake, CloudStorage
import shutil
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

def clean_data():
    try:
        shutil.rmtree('data')
    except:
        pass
        
def listening_sessions_job():
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
    sb = SongsBatchSource()
    datalake = CloudStorage(dir_path='data/songs')
    scdc = SongsCDC(source=sb, 
                    destination=datalake, 
                    syncFile='data/songs/sync.json', 
                    songs_to_request_dir='data/songs_to_req', 
                    key_attr='song_id')
    scdc.get_fresh_rows()

def shutdown_vm():
    credentials = GoogleCredentials.get_application_default()

    service = discovery.build('compute', 'v1', credentials=credentials)

    # Project ID for this request.
    project = 'lastfm-299413'  # Project ID
    # The name of the zone for this request.
    zone = 'europe-west6-a'  # Zone information

    # Name of the instance resource to stop.
    instance = '1784889149398741812'  # instance id

    request = service.instances().stop(project=project, zone=zone, instance=instance)
    response = request.execute()

if __name__ == "__main__":
    clean_data()
    listening_sessions_job()
    songs_cdc_job()
    shutdown_vm()