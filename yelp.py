import csv
import logging
import os

from configparser import ConfigParser
from datetime import date
from pathlib import Path
from time import time

import psycopg2
import psycopg2.extras
import requests

from requests import RequestException
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from db_utils import get_connection
from sql_commands import (
    GET_PAST_BUSINESS_SQL,
)

YELP_URL = "https://api.yelp.com/v3/businesses/search"
# max cache age for a listing
MAX_AGE = 60
HOME = str(Path.home())
CONFIG_FILE = os.path.join(HOME, ".browsing", "browser.conf")

config = ConfigParser()
config.read(CONFIG_FILE)
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    filename=config["logging"]["log_file"],
    filemode="a")


def get_api_key():
    """
    Read the API key from the configuration file.

    :return str: api_key
    """
    return config["yelp"]["api_key"]


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
    radius=600,
    categories=None,
    limit=50,
    session=None,
    timeout=5,
    url=YELP_URL,
    api_key=None,
):
    """
    Make a request to the Yelp Fusion API.

    """
    if not categories:
        categories = config["yelp"]["categories"].split(",")
    if not session:
        session = get_session()
    if not api_key:
        api_key = get_api_key()
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "radius": radius,
        "limit": limit,
        "categories": categories,
    }
    try:
        response = session.get(url, params=params, headers=headers)
        if response.status_code == 429:
            return "too many requests"
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        logging.error(e)
        return None


def get_number_businesses(
    latitude,
    longitude,
    radius=600,
    categories=None,
    api_key=None,
    session=None,
):
    """
    Query the Yelp Fusion API and return the number of businesses
    per category.
    """
    if not categories:
        categories = config["yelp"]["categories"].split(",")
    results = dict(zip(categories, [0] * len(categories)))
    data = query_yelp(
        latitude,
        longitude,
        radius=radius,
        categories=categories,
        api_key=api_key,
        session=session,
    )

    if data == "too many requests":
        return data

    elif data is None:
        return dict(zip(categories, ["NULL"] * len(categories)))

    businesses = data.get("businesses", [])
    for business in businesses:
        names = [v for d in business["categories"] for k, v in d.items()]
        for cat in categories:
            if cat in names:
                results[cat] += 1
    return results


def get_past_businesses(zipcode, burrough, address, connection=None):
    """
    Get the number of businesses previously recorded around an address.

    :param str zipcode:
    :param str burrough:
    :param str address:
    :param connection: connection object to the database
    :return dict: dictionary where keys are Yelp categories and values are
                  number of businesses in this category
    """
    if not connection:
        connection = get_connection()
    cur = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(GET_PAST_BUSINESS_SQL, (zipcode, burrough, address))
    return cur.fetchone()


def add_yelp_annotation(
    input_csv,
    output_csv,
    columns,
    api_key=None,
    max_age=MAX_AGE,
):
    """
    Copy a CSV file and add geolocation coordinates from the address.

    :param str input_csv: name of CSV file after geocoding of address
    :param str output_csv: name of CSV file with added Yelp annotation
    :param list[str] columns: columns of the output CSV file
    :param str api_key: API key for the Yelp API
    :param int max_age: maximum tolerated age of Yelp annotation, in days
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

            api_key = get_api_key()
            session = get_session()

            for row in reader:
                zipcode = row["zip"]
                burrough = row["burrough"]
                address = row["address"]
                latitude = row["latitude"]
                longitude = row["longitude"]

                # no geographical coordinates to work with
                if latitude == "NULL" or longitude == "NULL":
                    row["metrostations"] = "NULL"
                    row["buses"] = "NULL"
                    row["grocery"] = "NULL"
                    row["pharmacy"] = "NULL"
                    row["laundromat"] = "NULL"

                # we have geographical coordinates
                else:
                    # check if we've seen these coordinates in the past
                    with get_connection() as con:
                        businesses = get_past_businesses(
                            zipcode,
                            burrough,
                            address,
                            con,
                        )

                    # discard results if they are too old
                    if businesses:
                        collection_date = businesses["collection_date"]
                        if (date.today() - collection_date).days > max_age:
                            businesses = None

                    # if coordinates are new or too old, query Yelp
                    if not businesses:
                        businesses = get_number_businesses(
                            latitude,
                            longitude,
                            api_key=api_key,
                            session=session,
                        )
                        if businesses == "too many requests":
                            break

                    row["metrostations"] = businesses["metrostations"]
                    row["buses"] = businesses["buses"]
                    row["grocery"] = businesses["grocery"]
                    row["pharmacy"] = businesses["pharmacy"]
                    row["laundromat"] = businesses["laundromat"]

                writer.writerow(row)

    stop = time()
    elapsed = (stop - start) / 60
    logging.info(f"Yelp annotation took {elapsed:.2f} minutes")
