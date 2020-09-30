import functools
import logging
import math
import re
import sys

sys.path.insert(0, "../")

from neighborhood_burrough_mapping import NEIGHBORHOOD_BURROUGH_MAPPING

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)

BURROUGHS = {"bronx", "brooklyn", "new york", "queens", "staten island"}


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
    details = soup.find("div", {"class": "ds-home-details-chip"})
    value = details.find("span", {"class": "ds-value"})
    return string_to_float(value.text)


@safety_net
def get_neighborhood(soup):
    """
    Get neighborhood name from the tag soup.

    :param soup: BeautifulSoup object
    :return str: neighborhood name
    """
    # try to parse neighborhood from address
    address = get_address(soup)
    splitted = address.split(",")[-1].strip()
    if splitted.lower() not in BURROUGHS:
        return splitted


@safety_net
def get_address(soup):
    """
    Get street address from the tag soup.

    :param soup: BeautifulSoup object
    :return str: street address
    """
    container = soup.find("h1", {"class": "ds-address-container"})
    address = "".join(
        child.text for child in container.children).replace(u"\xa0", " ")
    found_zip = re.search(r",\sNY\s\d{5}", address)
    if found_zip:
        return address[:found_zip.span()[0]]
    return address


@safety_net
def get_burrough(soup):
    """
    Get burrough name from the tag soup.

    :param soup: BeautifulSoup object
    :return str: burrough name
    """
    # try to parse burrough from address
    address = get_address(soup)
    splitted = address.split(",")[-1].strip()
    if splitted.lower() in BURROUGHS:
        return splitted
    return NEIGHBORHOOD_BURROUGH_MAPPING.get(splitted.lower(), "NULL")


@safety_net
def get_zip(soup):
    """
    Get burrough name from the tag soup.

    :param soup: BeautifulSoup object
    :return str: zip code
    """
    container = soup.find("h1", {"class": "ds-address-container"})
    address = "".join(
        child.text for child in container.children).replace(u"\xa0", " ")
    found_zip = re.search(r",\sNY\s(\d{5})", address)
    if found_zip:
        return found_zip.group(1)


@safety_net
def get_property_type(soup):
    """
    Get property type (Studio, etc) from the tag soup.

    :param soup: BeautifulSoup object
    :return str: property type
    """
    return soup.find("i", {"class": "zsg-icon-buildings"}).next.next.next.text


@safety_net
def get_rep_name(soup):
    """
    Get representative's name from the tag soup.

    :param soup: BeautifulSoup object
    :return str: representative's name
    """
    return soup.find("span", {"class": "ds-listing-agent-display-name"}).text


@safety_net
def get_agency_url(soup):
    """
    Get agency's URL from the tag soup.

    :param soup: BeautifulSoup object
    :return str: agency's URL
    """
    return soup.find("span", {"class": "ds-listing-agent-business-name"}).text


@safety_net
def get_description(soup):
    """
    Get the home's description from the tag soup.

    :param soup: BeautifulSoup object
    :return str: home's description
    """
    overview = soup.find("div", {"class": "ds-overview-section"})
    text = overview.next.text
    return text.replace("\n", " ")


@safety_net
def get_amenities(soup):
    """
    Get amenities list from the tag soup.

    :param soup: BeautifulSoup object
    :return str: amenities
    """
    default_text = "contact manager"
    amenities_list = []
    for amenity in soup.find_all("li", {"class": "ds-home-fact-list-item"}):
        if "zsg-icon-snowflake" in amenity.next.attrs["class"]:
            text = amenity.next.next.next.next.text.lower()
            if text != default_text:
                amenities_list.append(f"cooling: {text}")
        elif "zsg-icon-heating" in amenity.next.attrs["class"]:
            text = amenity.next.next.next.next.text.lower()
            if text != default_text:
                amenities_list.append(f"heating: {text}")
        elif "zsg-icon-pets" in amenity.next.attrs["class"]:
            text = amenity.next.next.next.next.text.lower()
            if text != default_text:
                amenities_list.append(f"pets: {text}")
        elif "zsg-icon-parking" in amenity.next.attrs["class"]:
            text = amenity.next.next.next.next.text.lower()
            if text != default_text:
                amenities_list.append(f"parking: {text}")
        elif "zsg-icon-laundry" in amenity.next.attrs["class"]:
            text = amenity.next.next.next.next.text.lower()
            if text != default_text:
                amenities_list.append(f"laundry: {text}")
    return ", ".join(amenities_list)


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
    overview = soup.find("div", {"class": "ds-overview"})
    for div in overview.find_all("div"):
        if div.text == "Days listed":
            return string_to_int(div.next.next.next)


@safety_net
def get_property_size(soup):
    """
    Get the property size from the tag soup.

    :param soup: BeautifulSoup object
    :return int: property size
    """
    lst = list(soup.find(
        "h3", {"class": "ds-bed-bath-living-area-container"}).children)
    return string_to_int(lst[4].text.split()[0])


@safety_net
def get_year_built(soup):
    """
    Get the year the property was built from the tag soup.

    :param soup: BeautifulSoup object
    :return str: year the property was built
    """
    features = soup.find_all("h3")[1].next_sibling
    for f in features:
        if f.find("div").text.lower() == "build":
            return f.find("p").text.strip()


@safety_net
def get_bedrooms(soup):
    """
    Get the number of bedrooms from the tag soup.

    :param soup: BeautifulSoup object
    :return int: number of bedrooms
    """
    lst = list(soup.find(
        "h3", {"class": "ds-bed-bath-living-area-container"}).children)
    if "--" in lst[0].text:
        return 0
    return string_to_int(lst[0].text.split()[0])


@safety_net
def get_bathrooms(soup):
    """
    Get the number of bathrooms from the tag soup.

    :param soup: BeautifulSoup object
    :return int: number of bathrooms
    """
    lst = list(soup.find(
        "h3", {"class": "ds-bed-bath-living-area-container"}).children)
    string = lst[2].text.split()[0]
    if ".5" in string:
        return string_to_int(string.split(".")[0])
    return string_to_int(string)


@safety_net
def get_half_bathrooms(soup):
    """
    Get the number of half bathrooms from the tag soup.

    :param soup: BeautifulSoup object
    :return int: number of bathrooms
    """
    lst = list(soup.find(
        "h3", {"class": "ds-bed-bath-living-area-container"}).children)
    if ".5" in lst[2].text.split()[0]:
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
    features = soup.find_all("h3")[1].next_sibling
    for f in features:
        if f.find("div").text.lower() == "property type":
            return f.find("p").text.strip()


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
