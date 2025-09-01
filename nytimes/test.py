import logging
import pprint
import sys

from browse import *
from parse_soup import *

sys.path.insert(0, "..")

from tor_sqs_browser import Browser

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)

pp = pprint.PrettyPrinter(indent=4)


logging.info("initialize browser")
browser = Browser(
    "https://www.nytimes.com",
    get_browsable=wrapper_next_page,
    get_parsable=get_listings,
    soup_parser=parse_webpage,
)

logging.info("download listings page")
content = browser.download_page(BEGIN_RENT_LISTINGS)
logging.info("parsing html")
soup = browser.html_parser(content)
logging.info("parsing soup")
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
            sys.exit(0)
