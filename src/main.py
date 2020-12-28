from api_utilities import SongsBatchSource, UsersBatchSource
from cdc import ListeningSessionsCDC, SongsCDC
from destination import CloudDatalake
from datetime import datetime

def main():
    datalake = CloudDatalake('data/datalake_log')
    ub = UsersBatchSource('src/users/users.json', 'data/datalake_log/sync.json')
    lscdc = ListeningSessionsCDC(ub, datalake, 'data/datalake_log/sync.json', 'threshold', 'ts', 'data/datalake_log')
    lscdc.get_fresh_rows()

    sb = SongsBatchSource(f"data/datalake_log/{datetime.today().strftime('%Y%m%d')}.json")
    datalake = CloudDatalake('data/datalake_reg')
    scdc = SongsCDC(sb, datalake, 'data/datalake_reg/sync.json', 'song_id')
    scdc.get_fresh_rows()

if __name__ == "__main__":
    main()