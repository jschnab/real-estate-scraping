import functools
import logging
import math
import re

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
    aside = soup.find("aside")
    for span in aside.find_all("span"):
        if span.text[0] == "$":
            return string_to_float(span.text)


@safety_net
def get_neighborhood(soup):
    """
    Get neighborhood name from the tag soup.

    :param soup: BeautifulSoup object
    :return str: neighborhood name
    """
    aside = soup.find("aside")
    spans = aside.find_all("span")
    return spans[1].text.strip()


@safety_net
def get_address(soup):
    """
    Get street address from the tag soup.

    :param soup: BeautifulSoup object
    :return str: street address
    """
    aside = soup.find("aside")
    spans = aside.find_all("span")
    return spans[2].text.strip()


@safety_net
def get_burrough(soup):
    """
    Get burrough name from the tag soup.

    :param soup: BeautifulSoup object
    :return str: burrough name
    """
    aside = soup.find("aside")
    spans = aside.find_all("span")
    return spans[3].text.split(",")[0].strip()


@safety_net
def get_zip(soup):
    """
    Get burrough name from the tag soup.

    :param soup: BeautifulSoup object
    :return str: zip code
    """
    aside = soup.find("aside")
    spans = aside.find_all("span")
    address = spans[3].text
    match = re.search(r"\s([01]\d{4})", address)
    if match:
        return match.group(1)


@safety_net
def get_property_type(soup):
    """
    Get property type (Studio, etc) from the tag soup.

    :param soup: BeautifulSoup object
    :return str: property type
    """
    aside = soup.find("aside")
    spans = aside.find_all("span")
    home_details = list(spans[4])
    return str(home_details[-1])


@safety_net
def get_bath_deprecated(soup):
    """
    Get number of bathrooms from the tag soup.

    This function is deprecated.

    :param soup: BeautifulSoup object
    :return int: number of bathrooms
    """
    aside = soup.find("aside")
    spans = aside.find_all("span")
    home_details = list(spans[4])
    bath = home_details[2]
    match = re.search(r"(\d+)\s[bB]ath", bath)
    if match:
        return float(match.group(1))


@safety_net
def get_listing_type_deprecated(soup):
    """
    Get listing type (rental, etc) from the tag soup.

    This function is deprecated.

    :param soup: BeautifulSoup object
    :return str: listing type
    """
    aside = soup.find("aside")
    spans = aside.find_all("span")
    home_details = list(spans[4])
    return str(home_details[-1])


@safety_net
def get_rep_name(soup):
    """
    Get representative's name from the tag soup.

    :param soup: BeautifulSoup object
    :return str: representative's name
    """
    aside = soup.find("aside")
    spans = aside.find_all("span")
    for s in spans:
        if re.search("representative", s.text, re.IGNORECASE):
            rep_info = s.next_sibling
            return rep_info.find("span").text.strip()


@safety_net
def get_agency_url(soup):
    """
    Get agency's URL from the tag soup.

    :param soup: BeautifulSoup object
    :return str: agency's URL
    """
    aside = soup.find("aside")
    spans = aside.find_all("span")
    for s in spans:
        if re.search("representative", s.text, re.IGNORECASE):
            agency_info = s.next_sibling
            try:
                return agency_info.find("a").attrs["href"].strip()
            except KeyError:
                return agency_info.find("a").text.strip()


@safety_net
def get_description(soup):
    """
    Get the home's description from the tag soup.

    :param soup: BeautifulSoup object
    :return str: home's description
    """
    about = soup.find("h3")
    if len(about.parent.find_all("a")) == 0:
        return about.parent.find_all("p")[0].text
    if len(about.parent.find_all("a")) == 1:
        return about.parent.find_all("p")[2].text


@safety_net
def get_amenities(soup):
    """
    Get amenities list from the tag soup.

    :param soup: BeautifulSoup object
    :return str: amenities
    """
    paragraphs = soup.find_all("p")
    for p in paragraphs:
        if p.has_attr("aria-label"):
            return p.text


@safety_net
def get_common_charges(soup):
    """
    Get common charges from the tag soup.

    :param soup: BeautifulSoup object
    :return str: common charges
    """
    features = soup.find_all("h3")[1].next_sibling
    for f in features:
        if f.find("div").text.lower() == "cc/maintenance":
            return string_to_float(f.find("p").text.strip())


@safety_net
def get_monthly_taxes(soup):
    """
    Get monthly taxes from the tag soup.

    :param soup: BeautifulSoup object
    :return str: monthly taxes
    """
    features = soup.find_all("h3")[1].next_sibling
    for f in features:
        if f.find("div").text.lower() == "monthly taxes":
            return string_to_float(f.find("p").text.strip())


@safety_net
def get_days_listed(soup):
    """
    Get the number of days the property was listed from the tag soup.

    :param soup: BeautifulSoup object
    :return int: number of days listed
    """
    features = soup.find_all("h3")[1].next_sibling
    for f in features:
        if f.find("div").text.lower() == "listed":
            text = f.find("p").text.split()[0].lower().strip()
            if text == "today":
                return 0
            else:
                return string_to_int(text)


@safety_net
def get_property_size(soup):
    """
    Get the property size from the tag soup.

    :param soup: BeautifulSoup object
    :return int: property size
    """
    features = soup.find_all("h3")[1].next_sibling
    for f in features:
        if f.find("div").text.lower() == "size":
            return string_to_int(f.find("p").text.split()[0])


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
    features = soup.find_all("h3")[1].next_sibling
    for f in features:
        if f.find("div").text.lower().startswith("bedroom"):
            return string_to_int(f.find("p").text)


@safety_net
def get_bathrooms(soup):
    """
    Get the number of bathrooms from the tag soup.

    :param soup: BeautifulSoup object
    :return int: number of bathrooms
    """
    features = soup.find_all("h3")[1].next_sibling
    for f in features:
        if f.find("div").text.lower().startswith("bathroom"):
            return string_to_int(f.find("p").text)


@safety_net
def get_half_bathrooms(soup):
    """
    Get the number of half bathrooms from the tag soup.

    :param soup: BeautifulSoup object
    :return int: number of bathrooms
    """
    features = soup.find_all("h3")[1].next_sibling
    for f in features:
        if f.find("div").text.lower().startswith("half bath"):
            return string_to_int(f.find("p").text)


@safety_net
def get_rooms(soup):
    """
    Get the number of rooms from the tag soup.

    :param soup: BeautifulSoup object
    :return int: number of rooms
    """
    features = soup.find_all("h3")[1].next_sibling
    for f in features:
        if f.find("div").text.lower() == "rooms":
            return string_to_int(f.find("p").text)


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
