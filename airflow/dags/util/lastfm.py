import base64
import os
import json
import pandas as pd

import requests


class LastfmAPI:
    def __init__(self):
        self.client_id = os.environ.get('SPOTIFY_CLIENT_ID')
        self.client_secret = os.environ.get('SPOTIFY_SECRET')
        self.client_lastfm_key = os.environ.get('LASTFM_KEY')

    def __auth(self) -> str:
        auth_url = f'https://ws.audioscrobbler.com/2.0/?method=auth.gettoken&api_key={self.client_lastfm_key}&format=json'
        #auth_header = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
        #headers = {'Authorization': f'Basic {auth_header}'}
        #data = {'grant_type': 'client_credentials'}
        response = requests.get(auth_url)
        response_data = response.json()
        #print(response_data['token'])
        return response_data['token']

    def get_top_songs_by_country(self, country: str) -> dict:
        assert country in ['brazil', 'spain', 'portugal','argentina']
        token = self.__auth()
        api_url = f'https://ws.audioscrobbler.com/2.0/'
        # headers = {'Authorization': f'Bearer {token}'}
        params = {'method':'geo.gettoptracks', 'country': country, 'format': 'json', 'api_key':self.client_lastfm_key, 'limit': 100}
        response = requests.get(api_url, params=params)
        print(response.json())
        return response.json()