import json
import time

import requests
from bs4 import BeautifulSoup

from utils import LOGGER, make_headers, get_cookies

URL = (
    "https://www.realtor.com/realestateandhomes-detail/"
    "{permalink}?from=srp-list-card"
)
PROGRESS_FILE = "remaining_properties_get_description.json"
SLEEP_TIME = 20  # seconds


def make_request(url, headers):
    resp = requests.get(url, headers=headers)
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError:
        LOGGER.error(f"Error: status {resp.status_code}")
    return resp


def add_cookies_to_headers(headers, cookies):
    headers["Cookie"] = "; ".join(
        f"{co['name']}={co['value']}" for co in cookies
    )


def parse_description_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    data = json.loads(soup.find(id="__NEXT_DATA__").text)
    return data["props"]["pageProps"]["initialReduxState"]["propertyDetails"][
        "description"
    ]["text"]


def get_description(permalink, cookies):
    LOGGER.debug(f"Getting description for property: {permalink}")
    headers = make_headers("headers_descriptions")
    add_cookies_to_headers(headers, cookies)
    resp = make_request(URL.format(permalink=permalink), headers)
    status_code = resp.status_code
    if status_code == 200:
        soup = BeautifulSoup(resp.text, "html.parser")
        data = json.loads(soup.find(id="__NEXT_DATA__").text)
        desc = data["props"]["pageProps"]["initialReduxState"][
            "propertyDetails"
        ]["description"]["text"]
    else:
        desc = ""
    return status_code, desc


def get_work_in_progress(input_file):
    try:
        with open(PROGRESS_FILE) as fi:
            properties = json.load(fi)
    except FileNotFoundError:
        LOGGER.info("Progress file does not exist, reading from input file")
        with open(input_file) as fi:
            properties = [
                {
                    "property_id": prop["property_id"],
                    "permalink": prop["permalink"],
                }
                for prop in json.load(fi)
            ]
    return properties


def save_work_in_progress(properties):
    with open(PROGRESS_FILE, "w") as fi:
        json.dump(properties, fi)


def get_all_descriptions(properties):
    cookies = get_cookies()
    idx = 1
    n_props = len(properties)
    descriptions = []
    while len(properties) > 0:
        prop = properties.pop()
        permalink = prop["permalink"]
        LOGGER.info(f"{idx}/{n_props}: {permalink}")
        if "test" in permalink.lower():
            LOGGER.info("Looks like a honeypot, skipping")
            continue
        status, desc = get_description(permalink, cookies)
        if status != 200:
            break
        descriptions.append(
            {"property_id": prop["property_id"], "description": desc}
        )
        idx += 1
        time.sleep(SLEEP_TIME)

    return descriptions


def run(input_file, output_file):
    properties = get_work_in_progress(input_file)
    descriptions = []
    try:
        descriptions = get_all_descriptions(properties)
    except Exception as e:
        LOGGER.error(f"Error: {e}")
    LOGGER.info("Saving progress")
    save_work_in_progress(properties)
    LOGGER.info("Saving results")
    with open(output_file, "w") as fi:
        json.dump(descriptions, fi)
    LOGGER.info("Done")


def main():
    run(
        "data/properties-for-sale-2024-11-17.json",
        "data/properties-descriptions-2024-11-17.json",
    )


if __name__ == "__main__":
    main()
