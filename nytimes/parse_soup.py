import re


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


def get_neighborhood(soup):
    """
    Get neighborhood name from the tag soup.

    :param soup: BeautifulSoup object
    :return str: neighborhood name
    """
    aside = soup.find("aside")
    spans = aside.find_all("span")
    return spans[1].text.strip()


def get_address(soup):
    """
    Get street address from the tag soup.

    :param soup: BeautifulSoup object
    :return str: street address
    """
    aside = soup.find("aside")
    spans = aside.find_all("span")
    return spans[2].text.strip()


def get_burrough(soup):
    """
    Get burrough name from the tag soup.

    :param soup: BeautifulSoup object
    :return str: burrough name
    """
    aside = soup.find("aside")
    spans = aside.find_all("span")
    return spans[3].text.split()[0].strip(",")


def get_zip(soup):
    """
    Get burrough name from the tag soup.

    :param soup: BeautifulSoup object
    :return str: burrough name
    """
    aside = soup.find("aside")
    spans = aside.find_all("span")
    address = spans[3].text
    match = re.search(r"\s([01]\d{4})", address)
    if match:
        return match.group(1)


def get_property_type(soup):
    """
    Get property type (Studio, etc) from the tag soup.

    :param soup: BeautifulSoup object
    :return str: property type
    """
    aside = soup.find("aside")
    spans = aside.find_all("span")
    home_details = list(spans[4])
    return str(home_details[0])


def get_n_bath(soup):
    """
    Get number of bathrooms from the tag soup.

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


def get_listing_type(soup):
    """
    Get listing type (rental, etc) from the tag soup.

    :param soup: BeautifulSoup object
    :return str: listing type
    """
    aside = soup.find("aside")
    spans = aside.find_all("span")
    home_details = list(spans[4])
    bath = home_details[2]
    return str(home_details[-1])


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
            return rep_info.find("span").text


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
            return agency_info.find("a").attrs["href"]


def get_description(soup):
    """
    Get the home's description from the tag soup.

    :param soup: BeautifulSoup object
    :return str: home's description
    """
    about = soup.find("h3")
    paragraphs = about.parent.find_all("p")
    return paragraphs[2].text


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
