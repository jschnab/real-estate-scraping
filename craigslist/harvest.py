import os
import sys

from pathlib import Path

sys.path.insert(0, os.path.join(str(Path.home()), "real-estate-scraping"))

from craigslist.browse import get_listing_id
from selenium_browser import Browser


def main():
    crawler = Browser(
        base_url="https://newyork.craigslist.org",
        get_page_id=get_listing_id,
        wait_page_load=10,
        config_file="craigslist.conf",
    )
    crawler.harvest()
    crawler.close()


if __name__ == "__main__":
    main()
