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

from sql_commands import (
    GET_PAST_BUSINESS_SQL,
)

YELP_URL = "https://api.yelp.com/v3/businesses/search"
YELP_CATEGORIES = ["buses", "metrostations", "grocery", "pharmacy"]
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


def get_credentials():
    """
    Read credentials from the configuration file.

    :return tuple[str]: client_id, api_key
    """
    config = ConfigParser()
    config.read(CONFIG_FILE)
    return config["yelp"]["client_id"], config["yelp"]["api_key"]


def get_api_key():
    """
    Read the API key from the configuration file.

    :return str: api_key
    """
    home = str(Path.home())
    config = ConfigParser()
    config.read(os.path.join(home, ".browsing", "browser.conf"))
    return config["yelp"]["api_key"]


def get_connection(autocommit=False):
    """
    Get a connection to a PostgreSQL database.

    :param bool autocommit: if SQL commands should be automatically committed
                            (optional, default False)
    :return: connection object
    """
    config = ConfigParser()
    config.read(CONFIG_FILE)
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
    categories=YELP_CATEGORIES,
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
    categories=YELP_CATEGORIES,
    api_key=None,
    session=None,
):
    """
    Query the Yelp Fusion API and return the number of businesses
    per category.
    """
    results = dict(zip(categories, [0] * len(categories)))
    data = query_yelp(
        latitude,
        longitude,
        radius=radius,
        categories=categories,
        api_key=api_key,
        session=session,
    )

    if data == "too many requests" or data is None:
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
                quoting=csv.QUOTE_NONNUMERIC,
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

                if latitude == "NULL" or longitude == "NULL":
                    row["metrostations"] = "NULL"
                    row["buses"] = "NULL"
                    row["grocery"] = "NULL"
                    row["pharmacy"] = "NULL"

                else:
                #with get_connection() as con:
                #    businesses = get_past_businesses(
                #        zipcode,
                #        burrough,
                #        address,
                #        con,
                #    )
                #    # discard results if they are too old
                #    if businesses:
                #        collection_date = businesses["collection_date"]
                #        if (date.today() - collection_date).days > max_age:
                #            businesses = None

               # if not businesses:
                    businesses = get_number_businesses(
                        latitude,
                        longitude,
                        api_key=api_key,
                        session=session,
                    )
                    row["metrostations"] = businesses["metrostations"]
                    row["buses"] = businesses["buses"]
                    row["grocery"] = businesses["grocery"]
                    row["pharmacy"] = businesses["pharmacy"]

                writer.writerow(row)

    stop = time()
    elapsed = (stop - start) / 60
    logging.info(f"took {elapsed:.2f} minutes")
