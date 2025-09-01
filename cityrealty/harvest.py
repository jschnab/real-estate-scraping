import os
import sys

from pathlib import Path

sys.path.insert(0, os.path.join(str(Path.home()), "real-estate-scraping"))

from cityrealty.browse import get_listing_id
from selenium_browser import Browser


def main():
    crawler = Browser(
        base_url="https://www.cityrealty.com",
        get_page_id=get_listing_id,
        config_file="cityrealty.conf",
        check_can_fetch=False,
    )
    crawler.harvest()
    crawler.close()


if __name__ == "__main__":
    main()
