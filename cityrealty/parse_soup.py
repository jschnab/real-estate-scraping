import dateutil
import functools
import logging
import math
import re
import sys

sys.path.insert(0, "/home/jonathans/real-estate-scraping")

from neighborhood_burrough_mapping import NEIGHBORHOOD_BURROUGH_MAPPING

from datetime import datetime
from urllib.parse import urlparse

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)


def safety_net(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            logging.error(f"html tag parsing failed with '{func.__name__}'")
    return wrapper


def string_to_float(string):
    """
    Convert a string representing a number to a float.

    :param str string: string to convert
    :return float:
    """
    remap = {
        ord(","): None,
        ord("$"): None,
        ord("\xa0"): None,
    }
    clean = string.translate(remap)
    try:
        return float(clean)
    except ValueError:
        return float("nan")


def string_to_int(string):
    """
    Convert a string representing a number to a int.
    Return 'NaN' if the string can't be converted to an integer.

    :param str string: string to convert
    :return int|nan:
    """
    remap = {
        ord(","): None,
        ord("$"): None,
        ord("\xa0"): None,
    }
    clean = string.translate(remap)
    try:
        return int(clean)
    except ValueError:
        return float("nan")


def to_null(d):
    """
    Convert the NaN (not a number) or empty values of a dictionary to the
    string 'NULL'.

    :param dict d: dictionary to convert
    :return dict: dictionary where NaN and empty values are 'NULL'
    """
    for key, value in d.items():
        if isinstance(value, float):
            if math.isnan(value):
                d[key] = "NULL"
        elif isinstance(value, str):
            if not value:
                d[key] = "NULL"
        elif value is None:
            d[key] = "NULL"
    return d


@safety_net
def get_rent_price(soup):
    """
    Get the rental price from the tag soup.

    :param soup: BeautifulSoup object
    :return float: rental price
    """
    return string_to_float(soup.find("span", {"class": "price"}).text)


@safety_net
def get_neighborhood(soup):
    """
    Get neighborhood name from the tag soup.

    :param soup: BeautifulSoup object
    :return str: neighborhood name
    """
    parent = soup.find("i", {"class": "fa fa-map-signs"}).parent
    sibling = parent.next_sibling.next_sibling
    if sibling.name == "span":
        return sibling.find("a").text.strip()


@safety_net
def get_address(soup):
    """
    Get street address from the tag soup.

    :param soup: BeautifulSoup object
    :return str: street address
    """
    return soup.find("h1", {"class": "bld_title"}).text.strip()


@safety_net
def get_burrough(soup):
    """
    Get burrough name from the tag soup.

    :param soup: BeautifulSoup object
    :return str: burrough name
    """
    neighborhood = get_neighborhood(soup).lower()
    return NEIGHBORHOOD_BURROUGH_MAPPING.get(neighborhood, "NULL")


@safety_net
def get_zip(soup):
    """
    Get burrough name from the tag soup.

    :param soup: BeautifulSoup object
    :return str: zip code
    """
    # not available
    return "NULL"


@safety_net
def get_property_type(soup):
    """
    Get property type (Studio, etc) from the tag soup.

    :param soup: BeautifulSoup object
    :return str: property type
    """
    # not available
    return "NULL"


@safety_net
def get_rep_name(soup):
    """
    Get representative's name from the tag soup.

    :param soup: BeautifulSoup object
    :return str: representative's name
    """
    contact = soup.find("div", {"class": "contact-wrapper"})
    if contact:
        name = contact.find("span", {"class": "name"})
        if name:
            return name.text.strip()


@safety_net
def get_agency_url(soup):
    """
    Get agency's URL from the tag soup.

    :param soup: BeautifulSoup object
    :return str: agency's URL
    """
    return urlparse(soup.find("a", {"class": "website_link"})["href"]).netloc


@safety_net
def get_description(soup):
    """
    Get the home's description from the tag soup.

    :param soup: BeautifulSoup object
    :return str: home's description
    """
    text = soup.find("div", {"class": "wysiwyg"}).text.replace("\n", " ")
    return text.strip()


@safety_net
def get_amenities(soup):
    """
    Get amenities list from the tag soup.

    :param soup: BeautifulSoup object
    :return str: amenities
    """
    amenities = []
    amenities_section = soup.find("div", {"class": "amenities section"})
    if amenities_section:
        for li in amenities_section.find_all("li"):
            amenities.append(li.text.strip())
    building_features = soup.find("div", {"class": "building_features closed"})
    if building_features:
        for li in building_features.find_all("li"):
            amenities.append(li.text.strip())
    return ", ".join(amenities)


@safety_net
def get_common_charges(soup):
    """
    Get common charges from the tag soup.

    :param soup: BeautifulSoup object
    :return float: common charges
    """
    # not available
    return float("nan")


@safety_net
def get_monthly_taxes(soup):
    """
    Get monthly taxes from the tag soup.

    :param soup: BeautifulSoup object
    :return float: monthly taxes
    """
    # not available
    return float("nan")


@safety_net
def get_days_listed(soup):
    """
    Get the number of days the property was listed from the tag soup.

    :param soup: BeautifulSoup object
    :return int: number of days listed
    """
    div = soup.find("div", {"class": "_content _listed"})
    span = div.find("span")
    date_listed = dateutil.parser.parse(span.text.split("Listed")[1])
    return (datetime.today() - date_listed).days + 1  # start at 1


@safety_net
def get_property_size(soup):
    """
    Get the property size from the tag soup.

    :param soup: BeautifulSoup object
    :return int: property size
    """
    text = soup.find("span", {"class": "beds_baths"}).text
    match = re.search(r"((\d,)?\d+)\sft", text)
    if match:
        size = match.group(1)
        return string_to_int(size)


@safety_net
def get_year_built(soup):
    """
    Get the year the property was built from the tag soup.

    :param soup: BeautifulSoup object
    :return str: year the property was built
    """
    building_section = soup.find(
        "div",
        {"class": "lst_info section building_info"},
    )
    for li in building_section.find_all("li"):
        if li.text.startswith("Built in"):
            built_year = re.search(r"[12][890]\d\d", li.text).group(0)
            return string_to_int(built_year)


@safety_net
def get_bedrooms(soup):
    """
    Get the number of bedrooms from the tag soup.

    :param soup: BeautifulSoup object
    :return int: number of bedrooms
    """
    text = soup.find("span", {"class": "beds_baths"}).text
    match = re.search(r"(\d+)\+? bed", text)
    if match:
        beds = match.group(1)
        return string_to_int(beds)


@safety_net
def get_bathrooms(soup):
    """
    Get the number of bathrooms from the tag soup.

    :param soup: BeautifulSoup object
    :return float: number of bathrooms
    """
    text = soup.find("span", {"class": "beds_baths"}).text
    match = re.search(r"(\d+)(\.5)? bath", text)
    if match:
        baths = match.group(1)
        return string_to_int(baths)


@safety_net
def get_half_bathrooms(soup):
    """
    Get the number of half bathrooms from the tag soup.

    :param soup: BeautifulSoup object
    :return int: number of bathrooms
    """
    text = soup.find("span", {"class": "beds_baths"}).text
    match = re.search(r"\d+(\.5)? bath", text)
    if match and match.group(1) == ".5":
        return 1


@safety_net
def get_rooms(soup):
    """
    Get the number of rooms from the tag soup.

    :param soup: BeautifulSoup object
    :return int: number of rooms
    """
    # not available
    return float("nan")


@safety_net
def get_listing_type(soup):
    """
    Get the listing type (rental, etc) from the tag soup.

    :param soup: BeautifulSoup object
    :return str: property type
    """
    for span in soup.find_all("span"):
        if span.text == "Building Type":
            if span.next_sibling.next_sibling.name == "span":
                return span.next_sibling.next_sibling.text.strip()


def parse_webpage(soup):
    """
    Parse the contents of a HTML page.

    :param bytes page_content: HTML code
    :return dict: keys are home listing attributes and values are the
                  corresponding values parsed from HTML
    """
    results = {}
    results["listing_type"] = get_listing_type(soup)
    results["property_type"] = get_property_type(soup)
    results["burrough"] = get_burrough(soup)
    results["neighborhood"] = get_neighborhood(soup)
    results["address"] = get_address(soup)
    results["zip"] = get_zip(soup)
    results["price"] = get_rent_price(soup)
    results["description"] = get_description(soup)
    results["amenities"] = get_amenities(soup)
    results["common_charges"] = get_common_charges(soup)
    results["monthly_taxes"] = get_monthly_taxes(soup)
    results["days_listed"] = get_days_listed(soup)
    results["size"] = get_property_size(soup)
    results["year_built"] = get_year_built(soup)
    results["bedrooms"] = get_bedrooms(soup)
    results["bathrooms"] = get_bathrooms(soup)
    results["half_bathrooms"] = get_half_bathrooms(soup)
    results["rooms"] = get_rooms(soup)
    results["representative"] = get_rep_name(soup)
    results["agency"] = get_agency_url(soup)
    results = to_null(results)
    return results
