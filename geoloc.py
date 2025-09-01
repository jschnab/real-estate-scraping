import csv
import logging
import math
import os

from configparser import ConfigParser
from pathlib import Path
from time import time

import psycopg2
import psycopg2.extras
import requests

from db_utils import execute_sql, get_connection, table_exists
from geopy.geocoders import Nominatim
from requests import RequestException
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from sql_commands import (
    CHECK_CACHE_SQL,
    CREATE_CACHE_SQL,
    INSERT_CACHE_SQL,
    QUERY_CACHE_SQL,
)

BING_URL = (
    "http://dev.virtualearth.net/REST/v1/Locations?CountryRegion=US"
    "&adminDistrict=NY&postalCode={}&locality={}&addressLine={}&key={}"
)

HOME = str(Path.home())
CONFIG_FILE = os.path.join(HOME, ".browsing", "browser.conf")

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    filename=os.path.join(HOME, ".browsing", "browser.log"),
    filemode="a")


def check_cache(zipcode, burrough, address, connection):
    """
    Check if the address is in the geolocation cache.

    :param str zipcode:
    :param str burrough:
    :param str address:
    :param connection: connection object to the database
    :return bool: True if the address is cached, else False
    """
    cur = connection.cursor()
    cur.execute(CHECK_CACHE_SQL, (zipcode, burrough, address))
    return cur.fetchone()[0]


def query_cache(zipcode, burrough, address, connection):
    """
    Query an address from the geolocation cache.

    :param str zipcode:
    :param str burrough:
    :param str address:
    :param connection: connection object to the database
    :return tuple: (latitude, longitude) for the address
    """
    cur = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(QUERY_CACHE_SQL, (zipcode, burrough, address))
    return cur.fetchone()


def insert_cache(zipcode, burrough, address, lat, lon):
    """
    Insert an address and its coordinates in the cache.

    :param str zipcode:
    :param str burrough:
    :param str address:
    :param float lat: latitude of the address
    :param float lon: longitude of the address
    :return bool: True if the address is cached, else False
    """
    execute_sql(
        INSERT_CACHE_SQL,
        (zipcode, burrough, address, lat, lon),
    )


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
    try:
        resources_sets = response.get("resourceSets", [{}])
        resources = resources_sets[0].get("resources", [{}])
        geocodepoints = resources[0].get("geocodePoints", [{}])
        coordinates = geocodepoints[0].get("coordinates")
    except Exception as e:
        logging.error(f"{e}, cannot parse: {response}")
        coordinates = None
    if not coordinates:
        return float("nan"), float("nan")
    return coordinates[0], coordinates[1]


def query_bing_maps(
    zipcode,
    city,
    address,
    key=None,
    session=None,
    timeout=5,
    *args,
    **kwargs,
):
    """
    Query the Bing Maps location REST API and return unformatted results.

    Parameters will have their spaces replaced by '%20'.

    :param str zipcode:
    :param str city:
    :param str address:
    :param str key: API key for authentication
    :param session: requests session
    :return tuple: latitude, longitude for the address
    """
    if not session:
        session = get_session()
    if not key:
        config = ConfigParser()
        config.read(CONFIG_FILE)
        key = config["geolocation"]["bing_maps_key"]
    try:
        url = BING_URL.format(
            zipcode,
            city,
            address,
            key,
        ).replace(" ", "%20")
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        return parse_bing(response.json())
    except RequestException:
        return float("nan"), float("nan")


def query_nominatim(zipcode, city, address, *args, **kwargs):
    """
    Query the OpenStreetMap geocoding service and return unformatted results.

    :param str zipcode:
    :param str city:
    :param str address:
    :param session: requests session
    :return dict: BingMaps API response
    """
    locator = Nominatim(user_agent="real-estate-browsing", timeout=3)
    query = f"{address} {zipcode} {city}"
    try:
        location = locator.geocode(query)
        return location.latitude, location.longitude
    except Exception:
        return float("nan"), float("nan")


def add_coordinates(
    input_csv,
    output_csv,
    columns,
    geocode=query_bing_maps,
    api_key=None,
):
    """
    Copy a CSV file and add geolocation coordinates from the address.

    :param str input_csv: name of CSV file containing only the address
    :param str output_csv: name of CSV file with added latitude and longitude
    :param list[str] columns: columns of the output CSV file
    :param callable geocode: function which accepts a zipcode, a city,
                             an address, *args and *kwargs and returns
                             a dictionary containing latitude and longitude
    :param str api_key: API key for the geocoding API
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

            for row in reader:
                zipcode = row["zip"]
                burrough = row["burrough"]
                # for NY Times
                if " unit " in row["address"]:
                    # following line should strip, need to clean database
                    address = row["address"].split("unit")[0]
                # for CityRealty and Zillow
                elif " #" in row["address"]:
                    # following line should strip, need to clean database
                    address = row["address"].split("#")[0]
                # for Zillow
                elif " APT " in row["address"]:
                    address = row["address"].split(" APT ")[0]
                else:
                    address = row["address"]
                with get_connection() as con:
                    if not table_exists("geocache"):
                        execute_sql(CREATE_CACHE_SQL)
                    cached = query_cache(zipcode, burrough, address, con)
                if cached:
                    lat, lon = cached["latitude"], cached["longitude"]
                else:
                    lat, lon = geocode(zipcode, burrough, address, api_key)
                    if not math.isnan(lat) and not math.isnan(lon):
                        insert_cache(zipcode, burrough, address, lat, lon)
                if math.isnan(lat):
                    lat = "NULL"
                if math.isnan(lon):
                    lon = "NULL"
                row["latitude"] = lat
                row["longitude"] = lon
                writer.writerow(row)
    stop = time()
    elapsed = (stop - start) / 60
    print(f"geolocation took {elapsed:.2f} minutes")
