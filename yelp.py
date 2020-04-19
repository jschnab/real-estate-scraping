import os

from configparser import ConfigParser
from pathlib import Path

import requests

from requests import RequestException
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

YELP_URL = "https://api.yelp.com/v3/businesses/search"


def get_credentials():
    """
    Read credentials from the configuration file.

    :return tuple[str]: client_id, api_key
    """
    home = str(Path.home())
    config = ConfigParser()
    config.read(os.path.join(home, ".browsing", "browser.conf"))
    return config["yelp"]["client_id"], config["yelp"]["api_key"]


def get_session(
    max_retries=5,
    backoff_factor=0.3,
    retry_on=(500, 502, 503, 504),
):
    """
    Setup a requests session with retries.

    :param int max_retries: maximum number of retries when requests fail
    :param int backoff_factor: for exponential backoff when requests fail
    :param tup[int] retry_on: HTTP status codes which we force retry on
    :return requests.Session:
    """
    session = requests.Session()
    retry = Retry(
        total=max_retries,
        read=max_retries,
        connect=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=retry_on,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def query_yelp(
    latitude,
    longitude,
    radius=700,
    categories=["busstations", "metrostations", "grocery", "pharmacy"],
    limit=50,
    session=None,
    timeout=5,
    url=YELP_URL,
    api_key=None,
):
    """
    Make a request to the Yelp Fusion API.

    """
    if not session:
        session = get_session()
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "radius": radius,
        "limit": limit,
        "categories": categories,
    }
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        print(e)
        return None
