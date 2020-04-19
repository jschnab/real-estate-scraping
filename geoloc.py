import csv
import math
import os

from configparser import ConfigParser
from pathlib import Path
from time import time

import psycopg2
import requests

from geopy.geocoders import Nominatim
from requests import RequestException
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from sql_commands import (
    CHECK_CACHE_SQL,
    INSERT_CACHE_SQL,
    QUERY_CACHE_SQL,
)

CONFIG_DIR = os.path.join(Path.home(), ".browsing")
BING_URL = (
    "http://dev.virtualearth.net/REST/v1/Locations?CountryRegion=US"
    "&adminDistrict=NY&postalCode={}&locality={}&addressLine={}&key={}"
)


def get_connection(autocommit=False):
    """
    Get a connection to a PostgreSQL database.

    :param bool autocommit: if SQL commands should be automatically committed
                            (optional, default False)
    :return: connection object
    """
    config = ConfigParser()
    config.read(os.path.join(CONFIG_DIR, "browser.conf"))
    database = config["database"]["database_name"]
    username = config["database"]["username"]
    password = config["database"]["password"]
    host = config["database"]["host"]
    port = int(config["database"]["port"])
    con = psycopg2.connect(
        database=database,
        user=username,
        password=password,
        host=host,
        port=port,
    )
    con.autocommit = autocommit
    return con


def check_cache(postcode, city, address, connection):
    """
    Check if the address is in the geolocation cache.

    :param str postcode:
    :param str city:
    :param str address:
    :param connection: connection object to the database
    :return bool: True if the address is cached, else False
    """
    cur = connection.cursor()
    cur.execute(CHECK_CACHE_SQL, (postcode, city, address))
    return cur.fetchone()[0]


def query_cache(postcode, city, address, connection):
    """
    Query an address from the geolocation cache.

    :param str postcode:
    :param str city:
    :param str address:
    :param connection: connection object to the database
    :return tuple: (lat, lon) for the address
    """
    cur = connection.cursor()
    cur.execute(QUERY_CACHE_SQL, (postcode, city, address))
    return cur.fetchone()


def insert_cache(postcode, city, address, lat, lon, connection):
    """
    Insert an address and its coordinates in the cache.

    :param str postcode:
    :param str city:
    :param str address:
    :param float lat: latitude of the address
    :param float lon: longitude of the address
    :param connection: connection object to the database
    :return bool: True if the address is cached, else False
    """
    cur = connection.cursor()
    cur.execute(INSERT_CACHE_SQL, (postcode, city, address, lat, lon))


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


def parse_bing(response):
    """
    Parse the response from the Bing Maps API and extracts the latitude and
    longitude.

    :param dict response: Bing Maps response
    :return tuple[float]: latitude, longitude
    """
    resources_sets = response.get("resourceSets", [{}])
    resources = resources_sets[0].get("resources", [{}])
    geocodepoints = resources[0].get("geocodePoints", [{}])
    coordinates = geocodepoints[0].get("coordinates", [])
    if not coordinates:
        return float("nan"), float("nan")
    else:
        return coordinates[0], coordinates[1]


def query_bing_maps(
    postcode,
    city,
    address,
    key,
    session=None,
    timeout=5,
    *args,
    **kwargs,
):
    """
    Query the Bing Maps location REST API and return unformatted results.

    Parameters will have their spaces replaced by '%20'.

    :param str postcode:
    :param str city:
    :param str address:
    :param str key: API key for authentication
    :param session: requests session
    :return tuple: latitude, longitude for the address
    """
    if not session:
        session = get_session()
    try:
        url = BING_URL.format(
            postcode,
            city,
            address,
            key,
        ).replace(" ", "%20")
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        return parse_bing(response.json())
    except RequestException:
        return float("nan"), float("nan")


def query_nominatim(postcode, city, address, *args, **kwargs):
    """
    Query the OpenStreetMap geocoding service and return unformatted results.

    :param str postcode:
    :param str city:
    :param str address:
    :param session: requests session
    :return dict: BingMaps API response
    """
    locator = Nominatim(user_agent="python", timeout=3)
    query = f"{address.split('unit')[0]} {postcode} {city}"
    try:
        location = locator.geocode(query)
        return location.latitude, location.longitude
    except Exception:
        return float("nan"), float("nan")


def add_coordinates(
    input_csv,
    output_csv,
    columns,
    api_key,
    geocode,
):
    """
    Copy a CSV file and add geolocation coordinates from the address.

    :param str input_csv: CSV file containing only the address
    :param str output_csv: CSV file with added latitude and longitude
    :param list[str] columns: columns of the output CSV file
    :param str api_key: API key for the geocoding API
    :param callable geocode: function which accepts a postcode, a city,
                             an address, *args and *kwargs and returns
                             a tuple of latitude and longitude
    """
    start = time()
    with open(input_csv) as infile:
        reader = csv.DictReader(infile, lineterminator=os.linesep)

        with open(output_csv, "w") as outfile:
            writer = csv.DictWriter(
                outfile,
                columns,
                lineterminator=os.linesep,
            )
            writer.writeheader()

            for index, row in enumerate(reader):
                postcode = row["zip"]
                city = row["burrough"]
                address = row["address"]
                with get_connection() as con:
                    cached = query_cache(postcode, city, address, con)
                    if not cached:
                        lat, lon = geocode(postcode, city, address, api_key)
                        insert_cache(
                            postcode,
                            city,
                            address,
                            lat,
                            lon,
                            con,
                        )
                    else:
                        lat, lon = cached[0], cached[1]
                if math.isnan(lat):
                    lat = "NULL"
                if math.isnan(lon):
                    lon = "NULL"
                row["latitude"] = lat
                row["longitude"] = lon
                writer.writerow(row)
    stop = time()
    elapsed = (stop - start) / 60
    print(f"took {elapsed:.2f} minutes")