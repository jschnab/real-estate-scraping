import logging
import random
import re

from string import ascii_letters
from urllib.parse import urljoin

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)

BASE_URL = "https://www.zillow.com"

BEGIN_RENT_LISTINGS = "https://www.zillow.com/new-york-ny/rentals/"

LISTING_PREFIX = urljoin(BASE_URL, "homedetails")


def is_valid_listings(link):
    """
    Determine if the link references a page containing homes listings.

    :param link: BeautifulSoup 'a' tag
    :return bool: True if the link should be followed, else False
    """
    if link.has_attr("href") and link.attrs["href"].startswith(LISTING_PREFIX):
        return True
    return False


def get_listings(soup):
    """
    Get NY Times real estate listings links from a web page.

    :param soup: web page contents parsed with BeautifulSoup
    :param callable is_valid: determines if the link is of interest
    :return list[str]: list of URLs of specific listings (should be appended
                       to the base URL)
    """
    listings = []
    for link in soup.find_all("a"):
        if is_valid_listings(link):
            listings.append(link.attrs["href"])
    return listings


def get_next_page(url):
    """
    Get next page containing real estate listings to crawl from
    the current page's URL.

    :param str url: current URL
    :return str: URL of the next page containing listings to crawl
    """
    match = re.search(r".*/(\d+)_p/", url)
    if match:
        next_number = int(match.group(1)) + 1
        next_url = urljoin(BEGIN_RENT_LISTINGS, f"{next_number}_p/")
        return next_url
    else:
        # the first page has no page index
        return urljoin(BEGIN_RENT_LISTINGS, "2_p/")


def wrapper_next_page(url):
    """
    Wrap `get_next_page()` to return a list so it's compatible with
    the Crawler class expectation for `get_crawlable`

    :param str url: URL to pass to `get_next_page()`
    :return list[str]: list of URLs to crawl
    """
    next_url = get_next_page(url)
    if not next_url:
        return []
    return [next_url]


def is_last_page(soup):
    """
    Determines if the current page is the last one showing listings.

    :param soup: web page contents parsed with BeautifulSoup
    :return bool: True if we're on the last page of listings, else False
    """
    meta = soup.find("meta", {"property": "og:url"})
    if meta.has_attr("content"):
        page_number = re.search(r".*/(\d+)_p/", meta.attrs["content"])
        if page_number:
            return page_number.group(1) == "20"
        if re.search(r"/rentals/$", meta.attrs["content"]):
            return False  # we are at the first page
    logging.warning("cannot determine if last page was reached, stopping")
    return True


def get_listing_id(url):
    """
    Get the real estate listing ID from the URL.
    If parsing the ID fails, we return a random string.

    :param str url: listing URL
    :return str: listing ID or random string
    """
    match = re.search(r"\/([\dA-Za-z]*)_zpid", url)
    if match:
        return match.group(1)
    else:
        return "".join(random.choice(ascii_letters) for _ in range(10))
