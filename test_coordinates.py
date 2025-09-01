import os

from configparser import ConfigParser
from pathlib import Path

from geoloc import *

HOME = str(Path.home())

config = ConfigParser()
config.read(os.path.join(HOME, '.browsing', 'browser.conf'))

columns = config['geolocation']['csv_header'].split(',')
api_key = config['geolocation']['bing_maps_key']

add_coordinates(
    input_csv=os.path.join(HOME, 'real-estate-scraping', 'extract.csv'),
    output_csv=os.path.join(HOME, 'real-estate-scraping', 'coordinates.csv'),
    columns=columns,
    geocode=query_bing_maps,
    api_key=api_key,
)
