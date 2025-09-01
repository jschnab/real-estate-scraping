import logging
import random
import re

from string import ascii_letters
from urllib.parse import urljoin

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)

BASE_URL = "https://newyork.craigslist.org"

BEGIN_RENT_LISTINGS = urljoin(
    BASE_URL,
    "/d/apartments-housing-for-rent/search/apa",
)


def is_valid_listings(link):
    """
    Determine if the link references a page containing homes listings.
    This function is not necessary for the moment, but this may change
    so we keep it to be eventually implemented.

    :param link: BeautifulSoup 'a' tag
    :return bool: True if the link should be followed, else False
    """
    raise NotImplementedError


def get_listings(soup):
    """
    Get Craigslist real estate listings links from a web page.

    :param soup: web page contents parsed with BeautifulSoup
    :param callable is_valid: determines if the link is of interest
    :return list[str]: list of URLs of rental listings
    """
    listings = []
    for link in soup.find_all("a", {"class": "result-title hdrlnk"}):
        listings.append(link.attrs["href"])
    return listings


def get_next_page_from_url(url):
    """
    Get next page containing real estate listings to crawl from
    the current page's URL.

    :param str url: current URL
    :return str: URL of the next page containing listings to crawl
    """
    match = re.search(r"\?s=(\d+)", url)
    if match:
        next_number = int(match.group(1)) + 120
        return f"{BEGIN_RENT_LISTINGS}?s={next_number}"
    else:
        # the first page has no page index
        return f"{BEGIN_RENT_LISTINGS}?s=120"


def wrapper_next_page(url):
    """
    Wrap `get_next_page()` to return a list so it's compatible with
    the Browser class expectation for `get_crawlable`

    :param str url: URL to pass to `get_next_page()`
    :return list[str]: list of URLs to crawl
    """
    next_url = get_next_page(url)
    if not next_url:
        return []
    return [next_url]


def get_next_page(soup):
    """
    Get next page containing real estate listings to browse from the soup.

    :param BeautifulSoup.soup: soup parsed from a web page
    :return list[str]: list or URLs to browse
    """
    next_pages = [
        link.attrs["href"]
        for link in soup.find_all("link", {"rel": "next"})
    ]
    return next_pages


def is_last_page(soup):
    """
    Determines if the current page is the last one showing listings.

    :param soup: web page contents parsed with BeautifulSoup
    :return bool: True if we're on the last page of listings, else False
    """
    button_next = soup.find("a", {"class": "button next"})
    if button_next.attrs["href"] == "":
        return True
    return False


def is_last_page_dummy(soup):
    """
    Determines if the current page is the last one showing listings.

    :param soup: web page contents parsed with BeautifulSoup
    :return bool: True if we're on the last page of listings, else False
    """
    button_next = soup.find("a", {"class": "button next"})
    if button_next.attrs["href"] == (
        "/d/apartments-housing-for-rent/search/apa?s=120"
    ):
        return True
    return False


def get_listing_id(url):
    """
    Get the real estate listing ID from the URL.
    If parsing the ID fails, we return a random string.

    :param str url: listing URL
    :return str: listing ID or random string
    """
    match = re.search(r"/(\d+)\.html$", url)
    if match:
        return match.group(1)
    else:
        return "".join(random.choice(ascii_letters) for _ in range(10))
