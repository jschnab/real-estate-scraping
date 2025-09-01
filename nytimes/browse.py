import random
import re

from string import ascii_letters

BASE_URL = "https://www.nytimes.com"

BEGIN_RENT_LISTINGS = (
    "https://www.nytimes.com/real-estate/homes-for-rent?channel=rent"
    "&locations%5B%5D=new-york-ny-usa&locations%5B%5D=brooklyn-ny-usa"
    "&locations%5B%5D=queens-ny-usa&locations%5B%5D=bronx-ny-usa"
    "&locations%5B%5D=staten-island-ny-usa"
)

LISTING_PREFIX = "/real-estate/usa/ny/"
INVALID_SUFFIX = "homes-for-rent"


def is_valid_listings(link):
    """
    Determine if the link references a page containing homes listings.

    :param link: BeautifulSoup 'a' tag
    :return bool: True if the link should be followed, else False
    """
    if all([
        link.attrs["href"].startswith(LISTING_PREFIX),
        not link.attrs["href"].endswith(INVALID_SUFFIX),
    ]):
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
    match = re.search(r"&p=\d+", url)
    if match:
        group = match.group(0)
        next_number = int(group.split("=")[-1]) + 1
        next_url = url[:-len(group)] + f"&p={next_number}"
        return next_url
    # the first page has no page index
    else:
        return url + "&p=2"


def get_page_index(soup):
    """
    Get index of page containing real estate listings to crawl.

    :param str soup: current page contents parsed with BeautifulSoup
    :return str: URL of the next page containing listings to crawl
    """
    for b in soup.find_all("button"):
        if b.has_attr("class"):
            for name in b.attrs["class"]:
                if name == "active":
                    return int(b.text)


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
    for p in soup.find_all("p"):
        if p.text.endswith("Homes"):
            if p.text.split()[2] == p.text.split()[4]:
                return True
    return False


def get_listing_id(url):
    """
    Get the real estate listing ID from the URL.
    If parsing the ID fails, we return a random string.

    :param str url: listing URL
    :return str: listing ID or random string
    """
    match = re.search(r"\/([\dA-Z\-]*)$", url)
    if match:
        return match.group(1)
    else:
        return "".join(random.choice(ascii_letters) for _ in range(10))
