#!.venv/bin/python
import yaml
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict
from plexapi.server import PlexServer
import qbittorrentapi
import requests
import os
import argparse
import logging


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
    response = requests.get(f'{RADARR_URL}/api/v3/movie', headers=headers)
    radarr_df = pd.DataFrame(
        {
            'id_radarr': x['id'],
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
    
def get_sonarr_df(SONARR_URL: str, SONARR_API_KEY: str) -> pd.DataFrame:
    headers = {'X-Api-Key': SONARR_API_KEY}

    series_df = pd.DataFrame(
        {
            'seriesId': x['id'],
            'title': x['title'],
            'status': x['status'],
            'ended': x['ended'],
            'seriesfolder': x['path'], 
            'monitored': x['monitored'],
            'added': x['added'],
            'nextAiring': x.get('nextAiring'),
            'previousAiring': x.get('previousAiring')

        } 
        for x in requests.get(f'{SONARR_URL}/api/v3/series', headers=headers).json()
    )
    episode_df = pd.DataFrame(
        {
            'episodeFileId': x['id'],
            'seriesId': x['seriesId'],
            'path': x['path'],
            'size': x['size'],
            'inode': os.stat(x['path']).st_ino
        }
        for seriesId in series_df['seriesId']
        for x in requests.get(f'{SONARR_URL}/api/v3/episodefile', headers=headers, params={'seriesId': seriesId}).json()
    )

    sonarr_df = series_df.merge(episode_df, how='left', on='seriesId')
    return sonarr_df

def get_qbittorrent_df(QB_URL: str, QB_USERNAME: str, QB_PASSWORD: str) -> pd.DataFrame:
    qb = qbittorrentapi.Client(host=QB_URL, username=QB_USERNAME, password=QB_PASSWORD)
    qb.auth_log_in()
    qbittorrent_df = pd.DataFrame(
        {
            'hash_qbt': torrent['hash'],
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
    qb.auth_log_out()
    return qbittorrent_df

def prune_movies(radarr_df: pd.DataFrame, qbittorrent_df: pd.DataFrame, plex_df: pd.DataFrame, 
                 RADARR_URL: str, RADARR_API_KEY: str, QB_USERNAME: str, QB_PASSWORD: str, QB_URL: str, 
                 delete=False) -> pd.DataFrame:
    d1 = datetime.today().date() - timedelta(days = 365*1)
    d2 = datetime.today().date() - timedelta(days = 365*2)
    prune_movies_df = \
        radarr_df.merge(
            plex_df, 
            how='inner', 
            on='folder', 
            suffixes=('_radarr', '_plex')
        ) \
        .merge(
            qbittorrent_df, 
            how='left', 
            on='inode', 
            suffixes=('_radarr', '_qbt')
        ) \
        .query(f'last_viewed.isnull() & added_on < @d1 | last_viewed < @d2') \
        .reset_index(drop=True)
    
    # Delete movies from Radarr & qbt
    if delete:
        prune_movies_df['response_radarr'] = \
            prune_movies_df['id_radarr'] \
            .apply(lambda x : requests.delete(
                f'{RADARR_URL}/api/v3/movie/{x}?deleteFiles=true', 
                headers= {'X-Api-Key': RADARR_API_KEY}
                ) \
                .status_code
            )
        
        qb = qbittorrentapi.Client(host=QB_URL, username=QB_USERNAME, password=QB_PASSWORD)
        qb.auth_log_in()
        prune_movies_df['hash_qbt'].apply(lambda hash_qbt: qb.torrents_delete(delete_files=True, torrent_hashes=hash_qbt))
        qb.auth.log_out()

    else:
        prune_movies_df['response_radarr'] = 'Not Deleted'

    return prune_movies_df

def prune_series(sonarr_df: pd.DataFrame, qbittorrent_df: pd.DataFrame, plex_df: pd.DataFrame, 
                 SONARR_URL: str, SONARR_API_KEY: str, QB_USERNAME: str, QB_PASSWORD: str, QB_URL: str, 
                 delete=False) -> pd.DataFrame:
    d1 = datetime.today().date() - timedelta(days = 365*1)
    d2 = datetime.today().date() - timedelta(days = 365*2)
    prune_series_df = \
        sonarr_df.merge(
            plex_df,
            left_on='seriesfolder',
            right_on='folder',
            how='left',
            suffixes=('_sonarr', '_plex')
        ) \
        .merge(
            qbittorrent_df,
            on='inode',
            how='left',
            suffixes=('_sonarr', '_qbt')
        ) \
        .query(f'(last_viewed.isnull() & added_on < @d1) | (monitored == False & last_viewed < @d2)') \
        .reset_index(drop=True)

    if delete:

        prune_series_df['response_sonarr'] = \
            prune_series_df['seriesId'] \
            .apply(lambda x : requests.delete(
                f'{SONARR_URL}/api/v3/series/{x}?deleteFiles=true', 
                headers= {'X-Api-Key': SONARR_API_KEY}
                ) \
                .status_code
            )
        
        qb = qbittorrentapi.Client(host=QB_URL, username=QB_USERNAME, password=QB_PASSWORD)
        qb.auth_log_in()
        prune_series_df['hash_qbt'].apply(lambda hash_qbt: qb.torrents_delete(delete_files=True, torrent_hashes=hash_qbt))
        qb.auth.log_out()

    else:
        prune_series_df['response_sonarr'] = 'Not Deleted'

    return prune_series_df

def main(delete_media, prune_sonarr = True, prune_radarr = True) -> pd.DataFrame:
    if not os.path.exists('logs'):
        os.mkdir('logs')
        os.mkdir('logs/prune_movies')
        os.mkdir('logs/prune_series')

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
    plex_df.to_csv('plex_df.csv', index=False)

    # Get qBittorrent files
    qbittorrent_df = get_qbittorrent_df(QB_URL, QB_USERNAME, QB_PASSWORD)
    qbittorrent_df.to_csv('qbittorrent_df.csv', index=False)

    # Get Radarr movies
    radarr_df = get_radarr_df(RADARR_URL, RADARR_API_KEY)
    radarr_df.to_csv('radarr_df.csv', index=False)

    # Get Sonarr series
    sonarr_df = get_sonarr_df(SONARR_URL, SONARR_API_KEY)
    sonarr_df.to_csv('sonarr_df.csv', index=False)

    # Prune movies from Radarr & qBittorrent
    if prune_radarr:
        prune_movies_df = prune_movies(radarr_df, qbittorrent_df, plex_df,
                                       RADARR_URL, RADARR_API_KEY, QB_USERNAME, QB_PASSWORD, QB_URL,
                                       delete=delete_media)

        prune_movies_df.to_csv(f'logs/prune_movies/{datetime.now().date().isoformat()}.csv', index=False)
    else:
        prune_movies_df = pd.DataFrame(columns = ['id_type', 'id', 'title', 'folder', 'added_on', 'size', 'torrent', 'path_qbt'])

    if prune_sonarr:
        prune_series_df = prune_series(sonarr_df, qbittorrent_df, plex_df,
                                       SONARR_URL, SONARR_API_KEY, QB_USERNAME, QB_PASSWORD, QB_URL,
                                       delete=delete_media)
        
        prune_series_df.to_csv(f'logs/prune_series/{datetime.now().date().isoformat()}.csv', index=False)
    else:
        prune_series_df = pd.DataFrame(columns = ['id_type', 'id', 'title', 'folder', 'added_on', 'size', 'torrent', 'path_qbt'])

    prune_all_df = prune_series_df \
    .assign(id_type='seriesId') \
    .rename(columns = {'seriesId': 'id', 'title_sonarr': 'title', 'size_sonarr': 'size'}) \
    .reindex(columns = ['id_type', 'id', 'title', 'folder', 'added_on', 'size', 'torrent', 'path_qbt']) \
    .merge(
        prune_movies_df.assign(id_type='movieId') \
            .rename(columns={'id_radarr':'id', 'title_radarr': 'title', 'size_radarr': 'size'}) \
            .reindex(columns = ['id_type', 'id', 'title', 'folder', 'added_on', 'size', 'torrent', 'path_qbt']),
        how='outer'
    )


    return prune_all_df

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', 
                        help='Show info level logs', 
                        action='store_const',
                        dest='loglevel',
                        default=logging.WARNING,
                        const=logging.INFO) 
    parser.add_argument('-d', '--debug',
                        help='Show debug level logs',
                        action='store_const',
                        dest='loglevel',
                        const=logging.DEBUG)
    parser.add_argument('-r', '--remove', 
                        help='Remove stale media', 
                        action='store_true')
    parser.add_argument('--sonarr', 
                        help='Flag Sonarr media', 
                        action='store_true')
    parser.add_argument('--radarr',
                        help='Flag Radarr media',
                        action='store_true')
    parser.add_argument('-a', '--all',
                        help='Flag all media',
                        action='store_true')
    args = parser.parse_args()
    return args


if __name__ == '__main__':

    args = parse_args()

    logging.basicConfig(level=args.loglevel, 
                        format='%(asctime)s - %(levelname)s: %(message)s')
    logger = logging.getLogger()

    if args.remove:
        logger.warning('Flagging files for deletion.')
    else:
        logger.warning('TEST RUN!! No files will be deleted.')
    

    prune_all_df = main(args.remove, (args.sonarr or args.all), (args.radarr or args.all))
    prune_all_df.to_csv('prune_all_df.csv')
    print(f'''
    Movies deleted:
        Count: {len(prune_all_df[prune_all_df['id_type'] == 'movieId'])}
        Size:  {prune_all_df[prune_all_df['id_type'] == 'movieId']['size'].sum() / (1024**3):0.2f} GB

    Series deleted:
        Count: {len(prune_all_df[prune_all_df['id_type'] == 'seriesId']['id'].unique())}
        Size:  {prune_all_df[prune_all_df['id_type'] == 'seriesId']['size'].sum() / (1024**3):0.2f} GB
    ''')
    logger.info(f'''
    Movies deleted:
        {'\n        '.join(f'{row.title} ({row.size/1024**3:0.2f} GB)' for row in prune_all_df.query('id_type == "movieId"')[['title', 'size']].itertuples())}
    
    Series deleted:
        {'\n        '.join(f'{row.title} ({row.size/1024**3:0.2f} GB)' for row in prune_all_df.query('id_type == "seriesId"').reindex(columns=['title', 'size']).groupby('title').sum().reset_index().itertuples())}
    ''')
