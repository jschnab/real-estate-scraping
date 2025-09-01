import pprint
import sys

from browse import *
from parse_soup import *

sys.path.insert(0, "..")

from selenium_browser import Browser

pp = pprint.PrettyPrinter(indent=4)

browser = Browser(
    "https://www.cityrealty.com",
    get_browsable=wrapper_next_page,
    get_parsable=get_listings,
    soup_parser=parse_webpage,
    wait_page_load=30,
)


content = browser.download_page(BEGIN_RENT_LISTINGS)
soup = browser.html_parser(content)
to_parse = browser.get_parsable(soup)

for url in to_parse:
    logging.info(f"downloading {url}")
    content = browser.download_page(url)
    soup = browser.html_parser(content)
    data = browser.soup_parser(soup)
    pp.pprint(data)
    while True:
        cont = input("continue ? y/n: ")
        if cont == "y":
            break
        elif cont == "n":
            browser.close()
            sys.exit(0)

browser.close()
