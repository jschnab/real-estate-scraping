import json
import os
import random

from pathlib import Path

import constants as cst

from browser import Browser
from craigslist import browse, parse_soup
from download_managers import FirefoxDownloadManager
from utils import LocalExploredSet, LocalQueue

with open(os.path.join(cst.CONFIG_DIR, "user_agents")) as f:
    user_agent = json.load(f)[1]

with open(os.path.join(cst.CONFIG_DIR, "headers")) as f:
    headers = json.load(f)
headers["User-Agent"] = user_agent

with open(os.path.join(cst.CONFIG_DIR, "proxies")) as f:
    proxies = json.load(f)
proxy = random.choice(proxies)

manager = FirefoxDownloadManager(
    base_url=browse.BASE_URL,
    headers=headers,
    proxies=proxy,
    driver_path=os.path.join(str(Path.home()), ".browsing", "geckodriver"),
    options=[],
)

browse_queue = LocalQueue()
harvest_queue = LocalQueue()

browser = Browser(
    base_url=browse.BASE_URL,
    stop_test=browse.is_last_page,
    get_browsable=browse.wrapper_next_page,
    get_harvestable=browse.get_listings,
    get_page_id=browse.get_listing_id,
    explored_set=LocalExploredSet(),
    browse_queue=browse_queue,
    harvest_queue=harvest_queue,
    session_manager=manager,
    soup_parser=parse_soup.parse_webpage,
)
