import functools
import logging
import math
import re
import sys

from datetime import datetime
from string import punctuation

sys.path.insert(0, "../")

from neighborhood_burrough_mapping import NEIGHBORHOOD_BURROUGH_MAPPING

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)

BURROUGHS = {"bronx", "brooklyn", "new york", "queens", "staten island"}
ZIP_REGEX = r"\s(1\d{4})[\s,$(]"


def safety_net(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(
                f"html tag parsing failed with '{func.__name__}' "
                f"with error: {e}"
            )
    return wrapper


def clean_string(string):
    """
    Remove unwanted characters from strings (symbols, HTML characters, etc).

    :param str string: string to clean
    :returns: str - cleaned string
    """
    remap = {ord(c): None for c in ("$", ",", "\xa0")}
    return string.translate(remap).strip()


def string_to_float(string):
    """
    Convert a string representing a number to a float.

    :param str string: string to convert
    :return float:
    """
    clean = clean_string(string)
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
    clean = clean_string(string)
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
    price = soup.find("span", {"class": "price"})
    return string_to_float(price.text)


@safety_net
def get_location_from_title(soup):
    """
    Extract and clean location from rent title in the tag soup.

    :param soup: BeautifulSoup object
    :return str: location name
    """
    title = soup.find("span", {"class": "postingtitletext"})
    location = title.find("small").text.lower()
    location = location.replace("near ", "").strip()
    location = location.replace(" ny", "").strip()
    location = location.replace("prime ", "").strip()
    for char in ("/", "@", ","):
        if char in location:
            location = location.split(char)[0].strip()
    street_number = re.search(r"(\s\d+.*)$", location)
    if street_number:
        location = location[:len(street_number.group(0))]

    # remove punctuation characters
    remap = {ord(p): None for p in punctuation}
    location = location.translate(remap).strip()

    return location


@safety_net
def get_neighborhood(soup):
    """
    Get neighborhood name from the tag soup.

    :param soup: BeautifulSoup object
    :return str: neighborhood name
    """
    location = get_location_from_title(soup)
    if location not in BURROUGHS:
        return location.title()


@safety_net
def get_address(soup):
    """
    Get street address from the tag soup.

    :param soup: BeautifulSoup object
    :return str: street address
    """
    address = soup.find("div", {"class": "mapaddress"})
    if address is not None:
        address = address.text.lower()
        if " near " in address:
            return address.split(" near ")[0]
        return address


@safety_net
def get_burrough(soup):
    """
    Get burrough name from the tag soup.

    :param soup: BeautifulSoup object
    :return str: burrough name
    """
    location = get_location_from_title(soup)
    if location in BURROUGHS:
        return location.title()
    return NEIGHBORHOOD_BURROUGH_MAPPING.get(location.lower(), "NULL")


@safety_net
def get_zip(soup):
    """
    Get burrough name from the tag soup.

    :param soup: BeautifulSoup object
    :return str: zip code
    """
    # try to parse zipcode from address
    address = get_address(soup)
    if address is not None:
        zipcode = re.search(ZIP_REGEX, address)
        if zipcode:
            return zipcode.group(1)

    # try to parse zipcode from description
    description = get_description(soup)
    if description is not None:
        zipcode = re.search(ZIP_REGEX, description)
        if zipcode:
            return zipcode.group(1)


@safety_net
def get_property_type(soup):
    """
    Get property type (Studio, etc) from the tag soup.

    :param soup: BeautifulSoup object
    :return str: property type
    """
    # not available
    return


@safety_net
def get_rep_name(soup):
    """
    Get representative's name from the tag soup.

    :param soup: BeautifulSoup object
    :return str: representative's name
    """
    # not available
    return


@safety_net
def get_agency_url(soup):
    """
    Get agency's URL from the tag soup.

    :param soup: BeautifulSoup object
    :return str: agency's URL
    """
    # try to get agency URL from description
    description = get_description(soup)
    website = re.search(r"www\..*\.(com|net)", description)
    if website:
        return website.group(0)


@safety_net
def get_coordinates(soup):
    """
    Get latitude and longitude of the home from the map.

    :param soup: BeautifulSoup object
    :return tuple[float, float]: latitude and longitude
    """
    map_ = soup.find("div", {"id": "map"})
    latitude = string_to_float(map_.attrs["data-latitude"])
    longitude = string_to_float(map_.attrs["data-longitude"])
    return latitude, longitude


@safety_net
def get_description(soup):
    """
    Get the home's description from the tag soup.

    :param soup: BeautifulSoup object
    :return str: home's description
    """
    description = soup.find("section", {"id": "postingbody"}).text.strip()

    # remove unnecessary comments and characters
    description = description.replace("\n", " ").replace("\xa0", " ").strip()
    boilerplate = "qr code link to this post"
    if description.lower().startswith(boilerplate):
        description = description[len(boilerplate):].strip()

    # add latitude and longitude from map
    # because the low quality of address impairs the geolocation step
    lat, lon = get_coordinates(soup)
    if not math.isnan(lat) and not math.isnan(lon):
        description += f" lat:{lat};lon:{lon}"

    return description


@safety_net
def get_amenities(soup):
    """
    Get amenities list from the tag soup.

    :param soup: BeautifulSoup object
    :return str: amenities
    """
    amenities = []
    attributes = soup.find_all("p", {"class": "attrgroup"})[-1]
    for span in attributes.find_all("span"):
        amenities.append(span.text)
    return ", ".join(amenities)


@safety_net
def get_common_charges(soup):
    """
    Get common charges from the tag soup.

    :param soup: BeautifulSoup object
    :return str: common charges
    """
    # not available
    return


@safety_net
def get_monthly_taxes(soup):
    """
    Get monthly taxes from the tag soup.

    :param soup: BeautifulSoup object
    :return str: monthly taxes
    """
    # not available
    return


@safety_net
def get_days_listed(soup):
    """
    Get the number of days the property was listed from the tag soup.

    :param soup: BeautifulSoup object
    :return int: number of days listed
    """
    timestamp = soup.find("time", {"class": "date timeago"}).attrs["datetime"]
    date = datetime.strptime(timestamp.split("T")[0], "%Y-%m-%d")
    return (datetime.today() - date).days


@safety_net
def get_property_size(soup):
    """
    Get the property size from the tag soup.

    :param soup: BeautifulSoup object
    :return int: property size
    """
    attributes = soup.find("p", {"class": "attrgroup"})
    for span in attributes.find_all("span"):
        match = re.search(r"((\d+,)?\d+)ft2", span.text)
        if match:
            return string_to_int(match.group(1))


@safety_net
def get_year_built(soup):
    """
    Get the year the property was built from the tag soup.

    :param soup: BeautifulSoup object
    :return str: year the property was built
    """
    # not available
    return


@safety_net
def get_bedrooms(soup):
    """
    Get the number of bedrooms from the tag soup.

    :param soup: BeautifulSoup object
    :return int: number of bedrooms
    """
    attributes = soup.find("p", {"class": "attrgroup"})
    for span in attributes.find_all("span"):
        match = re.search(r"(\d+)([Bb][Rr])", span.text)
        if match:
            return string_to_int(match.group(1))


@safety_net
def get_bathrooms(soup):
    """
    Get the number of bathrooms from the tag soup.

    :param soup: BeautifulSoup object
    :return int: number of bathrooms
    """
    attributes = soup.find("p", {"class": "attrgroup"})
    for span in attributes.find_all("span"):
        match = re.search(r"(\d+)(\.5)?([Bb][Aa])", span.text)
        if match:
            return string_to_int(match.group(1))


@safety_net
def get_half_bathrooms(soup):
    """
    Get the number of half bathrooms from the tag soup.

    :param soup: BeautifulSoup object
    :return int: number of half bathrooms
    """
    attributes = soup.find("p", {"class": "attrgroup"})
    for span in attributes.find_all("span"):
        match = re.search(r"(\d+)(\.5)?([Bb][Aa])", span.text)
        if match and match.group(2):
            return 1
    return 0


@safety_net
def get_rooms(soup):
    """
    Get the number of rooms from the tag soup.

    :param soup: BeautifulSoup object
    :return int: number of rooms
    """
    # not available
    return


@safety_net
def get_listing_type(soup):
    """
    Get the listing type (rental, etc) from the tag soup.

    :param soup: BeautifulSoup object
    :return str: property type
    """
    # not available
    return


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
