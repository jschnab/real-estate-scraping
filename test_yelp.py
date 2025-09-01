import os

from configparser import ConfigParser
from pathlib import Path

from yelp import *

HOME = str(Path.home())

config = ConfigParser()
config.read(os.path.join(HOME, '.browsing', 'browser.conf'))
columns = config['yelp']['csv_header'].split(",")

add_yelp_annotation(
    input_csv=os.path.join(HOME, 'real-estate-scraping', 'coordinates.csv'),
    output_csv=os.path.join(HOME, 'real-estate-scraping', 'annotated.csv'),
    columns=columns,
)
