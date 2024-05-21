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
    for movie in plex.library.section('Movies').all():
        if movie.history() == []:
            d.append([
                movie.title,
                movie.addedAt.date(),
                movie.locations[0],
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
                round(os.path.getsize(movie.locations[0])/ (1024 * 1024 * 1024), 3),
                history.accountID,
                history.viewedAt.date()
                ]
            )


    movie_history_df = pd.DataFrame(d, columns = ['Title','AddedOn','Path', 'Size (GB)', 'AccountID', 'ViewedOn'])
    movie_history_df['AddedOn'] = pd.to_datetime(movie_history_df['AddedOn'])
    movie_history_df['ViewedOn'] = pd.to_datetime(movie_history_df['ViewedOn'])
    movie_history_df['AccountID'] = movie_history_df['AccountID'].astype('Int64')
    movie_history_df = movie_history_df.groupby(['Title','AddedOn','Path', 'Size (GB)']).agg({'AccountID': 'count', 'ViewedOn':'max'}).reset_index()
    movie_history_df = movie_history_df.rename(columns={'AccountID': 'ViewCount', 'ViewedOn': 'LastViewedOn'})
    return movie_history_df

# Filter movies & histories to find which need removal
def filter_movie_history(movie_history_df):
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

    plex = PlexServer(PLEX_URL, PLEX_TOKEN)

    # Get all plex movies & view counts
    movie_history_df = get_movie_history(plex)
    movie_history_df.to_csv('movie_history_df.csv')

    # Filter movies & histories to find which need removal
    remove_movies_df = filter_movie_history(movie_history_df)
    remove_movies_df.to_csv('remove_movies_df.csv')



if __name__ == '__main__':
    main()