import json
import os
import random

from configparser import ConfigParser

import constants as cst

from browser import Browser
from craigslist import browse, parse_soup
from download_managers import SimpleDownloadManager
from extract_managers import JsonLinesExtractStore
from harvest_managers import ZipHarvestStore
from utils import LocalExploredSet, LocalQueue

config = ConfigParser()
config.read(os.path.join(cst.CONFIG_DIR, "browser.conf"))
column_names = [
    col.strip()
    for col in config["extract"]["csv_header"].split(",")
]

with open(os.path.join(cst.CONFIG_DIR, "user_agents")) as f:
    user_agent = json.load(f)[1]

with open(os.path.join(cst.CONFIG_DIR, "headers")) as f:
    headers = json.load(f)
headers["User-Agent"] = user_agent

with open(os.path.join(cst.CONFIG_DIR, "proxies")) as f:
    proxies = json.load(f)
proxy = random.choice(proxies)

download_manager = SimpleDownloadManager(
    base_url=browse.BASE_URL,
    headers=headers,
    proxies=proxy,
    max_retries=5,
)

extract_store = JsonLinesExtractStore("craigslist/test1.csv")

harvest_store = ZipHarvestStore("craigslist")

browse_queue = LocalQueue()
harvest_queue = LocalQueue()

browser = Browser(
    base_url=browse.BASE_URL,
    stop_test=browse.is_last_page,
    get_browsable=browse.get_next_page,
    get_harvestable=browse.get_listings,
    get_page_id=browse.get_listing_id,
    explored_set=LocalExploredSet(),
    browse_queue=browse_queue,
    harvest_queue=harvest_queue,
    download_manager=download_manager,
    harvest_store=harvest_store,
    extract_store=extract_store,
    soup_parser=parse_soup.parse_webpage,
)
