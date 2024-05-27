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

# Get all plex media & view counts
def get_plex_df(PLEX_URL: str, PLEX_TOKEN: str) -> pd.DataFrame:
    plex = PlexServer(PLEX_URL, PLEX_TOKEN)

    plex_df = pd.DataFrame(
        {
            'title': item.title,
            'folder': item.locations[0] if item.type == 'show' else os.path.dirname(item.locations[0]),
            'last_viewed': max([x.viewedAt.date() for x in item.history()], default=None),
            'view_count': len([x.viewedAt.date() for x in item.history()]),
            'added_on': item.addedAt.date()
        }
        for library in ['Anime', 'TV Shows', 'Movies']
        for item in plex.library.section(library).all()
    )

    return plex_df

def get_radarr_df(RADARR_URL: str, RADARR_API_KEY: str) -> pd.DataFrame:
    headers = {'X-Api-Key': RADARR_API_KEY}
    response = requests.get(f"{RADARR_URL}/api/v3/movie", headers=headers)
    radarr_df = pd.DataFrame(
        {
            'radarr_id': x['id'],
            'title': x['title'],
            'size': x['sizeOnDisk'],
            'folder': x['folderName'],
            'path': x['movieFile']['path'],
            'inode': os.stat(x['movieFile']['path']).st_ino
        } 
        for x in response.json()
        if x['hasFile']
    )
    return radarr_df
    
def get_qbittorrent_df(QB_URL: str, QB_USERNAME: str, QB_PASSWORD: str) -> pd.DataFrame:
    qb = qbittorrentapi.Client(host=QB_URL, username=QB_USERNAME, password=QB_PASSWORD)
    qb.auth_log_in()
    qbittorrent_df = pd.DataFrame(
        {
            'torrent': torrent['name'],
            'path': torrent['content_path'] if not os.path.isdir(torrent['content_path']) 
                else f'{os.path.dirname(torrent['content_path'])}/{file['name']}'
        } 
        for torrent in qb.torrents_info()
        for file in qb.torrents_files(torrent['hash'])
        if torrent['completion_on'] != 0
    )
    qbittorrent_df['size'] = qbittorrent_df['path'].apply(lambda x: os.path.getsize(x))
    qbittorrent_df['inode'] = qbittorrent_df['path'].apply(lambda x: os.stat(x).st_ino)
    return qbittorrent_df

# Filter movies & histories to find which need removal
def filter_movie_history(movie_history_df: pd.DataFrame) -> pd.DataFrame:
    d1 = datetime.today().date() - timedelta(days = 365*1)
    d2 = datetime.today().date() - timedelta(days = 365*2)
    remove_movies_df = movie_history_df.query('LastViewedOn.isnull() & AddedOn < @d1 | LastViewedOn < @d2').reset_index(drop=True)
    return remove_movies_df

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

    # Get Plex files & history
    plex_df = get_plex_df(PLEX_URL, PLEX_TOKEN)
    plex_df.to_csv('plex_df.csv')

    # Get qBittorrent files
    qbittorrent_df = get_qbittorrent_df(QB_URL, QB_USERNAME, QB_PASSWORD)
    qbittorrent_df.to_csv('qbittorrent_df.csv')

    # Get Radarr movies
    radarr_movies_df = get_radarr_df(RADARR_URL, RADARR_API_KEY)
    radarr_movies_df.to_csv('radarr_movies_df.csv')


    # Filter movies & histories to find which need removal
    # remove_movies_df = filter_movie_history(movie_history_df)
    # remove_movies_df.to_csv('remove_movies_df.csv')

    # Collect Radarr entries & join
    # radarr_movies_df = get_radarr_movies(RADARR_URL, RADARR_API_KEY)
    # remove_movies_df = remove_movies_df.merge(radarr_movies_df[['id', 'folderName']], left_on=['FolderPath'], right_on=['folderName']) \
    #     .drop('folderName', axis=1) \
    #     .rename(columns={'id': 'radarr_id'})

    # Delete entries from Radarr
    # remove_movies_df['radarr_response'] = \
    #     remove_movies_df['radarr_id'] \
    #     .apply(lambda x : requests.delete(
    #         f"{RADARR_URL}/api/v3/movie/{x}?deleteFiles=true", 
    #         headers= {'X-Api-Key': RADARR_API_KEY}
    #         ) \
    #         .status_code
    #     )

if __name__ == '__main__':
    main()