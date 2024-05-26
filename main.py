import yaml
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict
from plexapi.server import PlexServer
import qbittorrentapi
import requests
import os


# Load configuration from YAML file
def load_config(filename: str = 'config.yaml') -> Dict:
    with open(filename, 'r') as file:
        return yaml.safe_load(file)

# Get all plex movies & view counts
def get_movie_history(plex):

    d = []
    for library in ['Movies', 'TV Shows', 'Anime']:
        for movie in plex.library.section(library).all():
            if movie.history() == []:
                d.append([
                    movie.title,
                    movie.addedAt.date(),
                    movie.locations[0],
                    os.path.dirname(movie.locations[0]), 
                    round(os.path.getsize(movie.locations[0])/ (1024 * 1024 * 1024), 3),
                    None,
                    None
                    ]
                )
            for history in movie.history():
                d.append([
                    movie.title,
                    movie.addedAt.date(),
                    movie.locations[0],
                    os.path.dirname(movie.locations[0]),
                    round(os.path.getsize(movie.locations[0])/ (1024 * 1024 * 1024), 3),
                    os.stat(movie.locations[0]).st_ino
                    history.accountID,
                    history.viewedAt.date()
                    ]
                )


    movie_history_df = pd.DataFrame(d, columns = ['Title','AddedOn','Path', 'FolderPath', 'Size (GB)', 'AccountID', 'ViewedOn'])
    movie_history_df['AddedOn'] = pd.to_datetime(movie_history_df['AddedOn'])
    movie_history_df['ViewedOn'] = pd.to_datetime(movie_history_df['ViewedOn'])
    movie_history_df['AccountID'] = movie_history_df['AccountID'].astype('Int64')
    movie_history_df = movie_history_df.groupby(['Title', 'AddedOn', 'Path', 'FolderPath', 'Size (GB)']).agg({'AccountID': 'count', 'ViewedOn':'max'}).reset_index()
    movie_history_df = movie_history_df.rename(columns={'AccountID': 'ViewCount', 'ViewedOn': 'LastViewedOn'})
    return movie_history_df

# Filter movies & histories to find which need removal
def filter_movie_history(movie_history_df):
    d1 = datetime.today().date() - timedelta(days = 365*1)
    d2 = datetime.today().date() - timedelta(days = 365*2)
    remove_movies_df = movie_history_df.query('LastViewedOn.isnull() & AddedOn < @d1 | LastViewedOn < @d2').reset_index(drop=True)
    return remove_movies_df
    
def get_radarr_movies(RADARR_URL, RADARR_API_KEY):
    headers = {'X-Api-Key': RADARR_API_KEY}
    response = requests.get(f"{RADARR_URL}/api/v3/movie", headers=headers)
    return pd.DataFrame(
        {column: x[column] for column in ['id', 'title', 'monitored', 'sizeOnDisk',  'path', 'folderName']} for x in response.json()
    )
    
def get_qbittorrent_files(QB_URL, QB_USERNAME, QB_PASSWORD):
    qb = qbittorrentapi.Client(host=QB_URL, username=QB_USERNAME, password=QB_PASSWORD)
    qb.auth_log_in()
    df = pd.DataFrame(
        {
            'torrent': torrent['name'],
            'path': torrent['content_path'] if not os.path.isdir(torrent['content_path']) else f'{os.path.dirname(torrent['content_path'])}/{file['name']}'
        } 
        for torrent in qb.torrents_info()
        for file in qb.torrents_files(torrent['hash'])
    )
    df['Size (GB)'] = df['path'].apply(lambda x: round(os.path.getsize(x) / (1024**3), 4))
    df['inode'] = df['path'].apply(lambda x: os.stat(x).st_ino)
    return df

def main():
    config = load_config()

    PLEX_URL = config['plex']['url']
    PLEX_TOKEN = config['plex']['token']
    QB_URL = config['qbittorrent']['url']
    QB_USERNAME = config['qbittorrent']['username']
    QB_PASSWORD = config['qbittorrent']['password']
    SONARR_URL = config['sonarr']['url']
    SONARR_API_KEY = config['sonarr']['api_key']
    RADARR_URL = config['radarr']['url']
    RADARR_API_KEY = config['radarr']['api_key']

    plex = PlexServer(PLEX_URL, PLEX_TOKEN)

    # Get all plex movies & view counts
    movie_history_df = get_movie_history(plex)
    movie_history_df.to_csv('movie_history_df.csv')

    # Get qBittorrent files
    qbittorrent_df = get_qbittorrent_files(QB_URL, QB_USERNAME, QB_PASSWORD)
    qbittorrent_df.to_csv('qbittorrent_df.csv')

    # Get Radarr movies
    radarr_movies_df = get_radarr_movies(RADARR_URL, RADARR_API_KEY)
    radarr_movies_df.to_csv('radarr_movies_df.csv')


    # Filter movies & histories to find which need removal
    remove_movies_df = filter_movie_history(movie_history_df)
    remove_movies_df.to_csv('remove_movies_df.csv')

    # Collect Radarr entries & join
    radarr_movies_df = get_radarr_movies(RADARR_URL, RADARR_API_KEY)
    remove_movies_df = remove_movies_df.merge(radarr_movies_df[['id', 'folderName']], left_on=['FolderPath'], right_on=['folderName']) \
        .drop('folderName', axis=1) \
        .rename(columns={'id': 'radarr_id'})

    # Delete entries from Radarr
    remove_movies_df['radarr_response'] = \
        remove_movies_df['radarr_id'] \
        .apply(lambda x : requests.delete(
            f"{RADARR_URL}/api/v3/movie/{x}?deleteFiles=true", 
            headers= {'X-Api-Key': RADARR_API_KEY}
            ) \
            .status_code
        )

if __name__ == '__main__':
    main()