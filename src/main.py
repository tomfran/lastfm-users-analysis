from api_utilities import SongsBatchSource, UsersBatchSource
from cdc import ListeningSessionsCDC, SongsCDC
from destination import CloudDatalake

def main():
    datalake = CloudDatalake('data/datalake_log')
    ub = UsersBatchSource('src/users/users.json', 'data/datalake_log/sync.json')
    lscdc = ListeningSessionsCDC(ub, datalake, 'data/datalake_log/sync.json', 'threshold', 'ts', 'data/datalake_log')
    lscdc.get_fresh_rows()

    sb = SongsBatchSource('data/datalake_log/20201226.json')
    datalake = CloudDatalake('data/datalake_reg')
    scdc = SongsCDC(sb, datalake, 'data/datalake_reg/sync.json', 'song_id')
    scdc.get_fresh_rows()

if __name__ == "__main__":
    main()