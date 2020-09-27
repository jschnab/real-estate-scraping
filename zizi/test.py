import sys
import time

import browse
import parse_soup

sys.path.insert(0, "/home/jonathans/real-estate-scraping")

from selenium_browser import Browser

BASE_URL = "https://www.zillow.com"
CONFIG_FILE = "/home/jonathans/.browsing/zillow.conf"

start = time.time()

print("initialize browser")
browser = Browser(
    base_url=BASE_URL,
    get_parsable=browse.get_listings,
    get_page_id=browse.get_listing_id,
    soup_parser=parse_soup.parse_webpage,
    wait_page_load=30,
    config_file=CONFIG_FILE,
)

print("start browsing")
content = browser.download_page(browse.BEGIN_RENT_LISTINGS)
soup = browser.html_parser(content)
to_parse = set(browser.get_parsable(soup))
print(f"found {len(to_parse)} pages to parse:", to_parse)

for i, url in enumerate(to_parse):
    if i > 3:
        break
    print("parsing page:", browser.get_page_id(url))
    content = browser.download_page(url)
    soup = browser.html_parser(content)
    data = browser.soup_parser(soup)
    print(data)

browser.webdriver.close()
stop = time.time()
elapsed = stop - start
print(f"took {elapsed/60:.2f} minutes")
