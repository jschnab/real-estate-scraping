import os
import sys

from pathlib import Path

sys.path.insert(0, os.path.join(str(Path.home()), "real-estate-scraping"))

import cityrealty.browse
import cityrealty.parse_soup

from selenium_browser import Browser

BASE_URL = "https://www.cityrealty.com"
CONFIG_FILE = "cityrealty.conf"


def browse():
    crawler = Browser(
        base_url=BASE_URL,
        stop_test=cityrealty.browse.is_last_page,
        get_browsable=cityrealty.browse.wrapper_next_page,
        get_parsable=cityrealty.browse.get_listings,
        get_page_id=cityrealty.browse.get_listing_id,
        check_can_fetch=False,
        config_file=CONFIG_FILE,
    )
    crawler.browse(cityrealty.browse.BEGIN_RENT_LISTINGS)
    crawler.close()


if __name__ == "__main__":
    browse()
