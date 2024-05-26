# Plex-Prune

Tool will use Plex watch history to remove old or stale media from qBittorrent, Sonarr, and Radarr. 

Files downloaded with *arr and qBittorrent are hardlinked, so tool uses inode number find common point & delete media from both Sonarr/Radarr and qBittorrent.

## Current removal criteria

Movies
- Added 1 year ago & never watched
- Last watched 2 years ago

TV Shows
- Added 1 year ago & never watched
- Shows that have ended and haven't been watched in 2 years