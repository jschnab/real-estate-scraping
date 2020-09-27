import os
import sys

from pathlib import Path

sys.path.insert(0, os.path.join(str(Path.home()), "real-estate-scraping"))

from zizi.browse import get_listing_id
from selenium_browser import Browser


def main():
    crawler = Browser(
        base_url="https://www.zillow.com",
        get_page_id=get_listing_id,
        wait_page_load=30,
        config_file="zillow.conf",
    )
    crawler.harvest()
    crawler.close()


if __name__ == "__main__":
    main()
