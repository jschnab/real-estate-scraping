import random
import re

from string import ascii_letters
from urllib.parse import urljoin

BASE_URL = "https://www.cityrealty.com"
BEGIN_RENT_LISTINGS = urljoin(
    BASE_URL,
    "/nyc/apartments-for-rent/search-results#?page=1"
)


def get_listings(soup):
    """
    Get CityRealy rental listings links from a web page.

    :param soup: web page contents parsed with BeautifulSoup
    :return list[str]: list of URLs of specific listings (should be appended
                       to the base URL)
    """
    links = []
    for span in soup.find_all("span"):
        if span.has_attr("class") and "lst_name" in span["class"]:
            link = span.find("a")
            if link.has_attr("href"):
                links.append(link["href"])
    return links


def get_next_page(url):
    """
    Get next page containing rental listings to crawl from
    the current page's URL.

    :param str url: current URL
    :return str: URL of the next page containing listings to crawl
    """
    match = re.search(r"page=\d+", url)
    group = match.group(0)
    next_number = int(group.split("=")[-1]) + 1
    next_url = url[:-len(group)] + f"page={next_number}"
    return next_url


def wrapper_next_page(url):
    """
    Wrap `get_next_page()` to return a list so it's compatible with
    the Browser class expectation for `get_browsable`

    :param str url: URL to pass to `get_next_page()`
    :return list[str]: list of URLs to browse
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
    for li in soup.find_all("li"):
        if li.has_attr("class") and li.attrs["class"] == ["next", "ng-hide"]:
            return True
    return False


def get_listing_id(url):
    """
    Get the real estate listing ID from the URL.
    If parsing the ID fails, we return a random string.

    :param str url: listing URL
    :return str: listing ID or random string
    """
    match = re.search(r"\/(\w+)$", url)
    if match:
        return match.group(1)
    else:
        return "".join(random.choice(ascii_letters) for _ in range(10))
