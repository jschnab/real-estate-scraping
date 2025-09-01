from datetime import datetime
from dateutil import parser
from itertools import chain
import re


def format_feature_value(key, value):
    """
    Format the value, depending on the key.
    For example, if key contains 'date', convert to date.

    :param str key:
    :param str value:
    :return str: formatted value
    """
    if 'date' in key:
        return string_to_date(value)
    if value[0] == '$':
        return int(value[1:])
    else:
        return value.lower()


def format_feature_key(key):
    """
    Format the feature key by:
        - removing unnecessary characters
        - convert spaces to underscore
        - converts to lowercase

    :param str key: text to format
    :return str: formatted text
    """
    formatted = key.replace('& ', '')\
                   .replace(' ', '_')\
                   .replace('/', '_per_')\
                   .lower()
    return formatted


def validate_date(date):
    """
    Validate the year of a converted date.
    If a date not containing a year is converted, a mistake could be
    made on the year. This functions checks the year is correct and
    adjusts it if necessary.

    :param datetime.datetime date: date to validate
    :return datetime.datetime: validated date
    """
    today = datetime.today()
    diff = date - today
    if diff.days > 180:
        date = datetime(date.year - 1, date.month, date.day)
    elif diff.days < -180:
        date = datetime(date.year + 1, date.month, date.day)
    return date


def string_to_date(date):
    """
    Converts a string representing a date to a datetime object.
    This is meant to be used to convert date of house availability.
    If the date cannot be parsed, None is returned.

    :param str date: string representation of a date
    :return datetime.datetime: date
    """
    try:
        converted = parser.parse(date)
        validated = validate_date(converted)
        return validated

    except ValueError:
        return


def string_to_number(num_type):
    """
    Convert a string to a number of the specified type (int or float).

    :param num_type type: Python numeric type, int or float
    :raise AssertionError: if the argument is not int or float
    :return: a function which converts a string to the specified numeric type
    """
    err_msg = f'Expected int or float, got: {num_type}'
    assert (num_type == int) or (num_type == float), err_msg

    def converter(string):
        """
        Function which converts a string into a numeric type.

        :param string str: string to be converted
        :return int|float: converted string
        """
        string_modif = string.replace(u',', u'.').replace(u'\xa0', u'')

        # empty strings or symbold ('--') would throw ValueError
        try:
            return num_type(string_modif)
        except ValueError:
            return float('nan')

    return converter


string_to_int = string_to_number(int)
string_to_float = string_to_number(float)


def get_address(soup):
    """
    Get address from the page containing home details.

    :param soup: BeautifulSoup object
    :return str: address
    """
    addr_container = soup.find('h1', {'class': 'ds-address-container'})
    children = addr_container.children
    addr_elements = [c.text.replace(u'\xa0', u' ') for c in children]
    address = ''.join(addr_elements)
    return address


def get_stats(soup):
    """
    Get number of days listed, of views, and of applications from the page
    containing home details.
    :param soup: BeautifulSoup object
    :return Dict[str: int]: statistics of the home details page
    """
    stats = soup.find_all('li', {'class': 'ds-overview-stat'})
    children = [list(c.children) for c in stats]
    stats_elements = list(chain.from_iterable(children))
    text_elements = [e.text for e in stats_elements]
    d = {}
    for i in range(0, len(text_elements) - 1, 2):
        key = format_feature_key(text_elements[i])
        d[key] = string_to_int(text_elements[i + 1])
    return d


def get_description(soup):
    """
    Get the description of the listing, if it exists.

    :param soup: BeautifulSoup object
    :return str: description of the home
    """
    for div in soup.find_all('div'):
        _class = div.attrs.get('class')
        if _class:
            if all([
                isinstance(_class, list),
                div.parent.attrs.get('class') == ['ds-overview-section'],
                re.match('Text-sc', _class[0])
            ]):
                return div.text


def get_status(soup):
    """
    Get the status for the listing, e.g. "apartment for rent".

    :param soup: BeautifulSoup object
    :return str: status of the home
    """
    status = soup.find('span', {'class': 'ds-status-details'}).text
    return status.lower()


def get_agent_info(soup):
    """
    Get information about the agent who manages the listing.

    This does not work: the relevant HTML tag cannot be found in the
    GET response.

    :param soup: BeautifulSOup object
    :return Dict: agent information
    """
    _ = soup.find('span', {'class': 'ds-listing-agent-display-name'})


def get_features(soup):
    """
    Get facts and features of the listing: date available, heating, ect.

    :param soup: BeautifulSoup object
    :return Dict: dictionary of feature: value
    """
    features = {}
    for i in soup.find_all('li', {'class': 'ds-home-fact-list-item'}):
        feat = i.text.split(':')
        key = format_feature_key(feat[0])
        val = format_feature_value(key, feat[1])
        features[key] = val
    return features
