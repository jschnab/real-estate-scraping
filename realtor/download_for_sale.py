#!/usr/bin/env python3

import json
import os
from datetime import datetime

import requests

import constants as cst
import load
from utils import LOGGER, make_headers, get_cookies

URL = (
    "https://www.realtor.com/api/v1/"
    "rdc_search_srp?client_id=rdc-search-for-sale-search&schema=vesta"
)
TEMP_FILE = "temp.json"
RESULTS_PATH = os.path.join(cst.DATA_DIR, "properties-for-sale-{date}.json")

REQ_VARIABLES = {
    "variables": {
        "geoSupportedSlug": "Lansing_MI",
        "query": {
            "primary": True,
            "status": ["for_sale"],
            "search_location": {"location": "Lansing, MI"},
        },
        "client_data": {"device_data": {"device_type": "desktop"}},
        "limit": 200,
        "offset": 0,
        "sort_type": "relevant",
        "bucket": {"sort": "fractal_v2.1.11"},
        "search_promotion": {
            "name": "CITY",
            "slots": [],
            "promoted_properties": [[]],
        },
    },
    "isClient": True,
    "visitor_id": "85826405-4fe7-44a0-a8ef-b4297a815904",
}


def main():
    LOGGER.info("Getting cookies")
    cookies = get_cookies(
        "realestateandhomes-search/Lansing_MI", "cookies_for_sale",
    )
    LOGGER.info("Got cookies")

    LOGGER.info("Preparing request headers")
    headers = make_headers("headers_for_sale")
    headers["Cookie"] = "; ".join(
        f"{co['name']}={co['value']}" for co in cookies
    )

    LOGGER.info("Preparing request payload")
    payload = build_payload(cookies)

    for co in cookies:
        if co["name"] == "__vst":
            payload["visitor_id"] = co["value"]

    offset = 0
    properties = []
    while True:
        LOGGER.info(f"Making request with offset: {offset}")
        resp = make_request(URL, headers, payload)
        if resp is None:
            LOGGER.info("Request response is None")
            break
        count = resp.json()["data"]["home_search"]["count"]
        LOGGER.info(f"Got {count} results")
        if count == 0:
            break
        properties.extend(get_properties(resp))
        LOGGER.info("Saving temporary results")
        save_results(properties, TEMP_FILE)
        offset += count
        payload["variables"]["offset"] = offset

    if properties != []:
        results_path = RESULTS_PATH.format(
            date=datetime.now().strftime("%Y-%m-%d")
        )
        LOGGER.info(f"Saving results to {results_path}")
        save_results(
            properties, results_path,
        )
        LOGGER.info("Saved results")

    if os.path.isfile(TEMP_FILE):
        LOGGER.info("Deleting temporary file")
        os.remove(TEMP_FILE)
        LOGGER.info("Deleted temporary file")

    LOGGER.info("Loading new properties for sale to the database")
    load.insert_properties_for_sale(results_path)

    LOGGER.info("Done")


def build_payload(cookies):
    with open("query_for_sale.graphql") as fi:
        query = fi.read()

    variables = REQ_VARIABLES.copy()

    # make sure visitor_id matches cookies
    for co in cookies:
        if co["name"] == "__vst":
            variables["visitor_id"] = co["value"]

    return {"query": query, **variables}


def make_request(url, headers, json):
    resp = requests.post(url, json=json, headers=headers)
    try:
        resp.raise_for_status()
        return resp
    except requests.exceptions.HTTPError:
        LOGGER.error(f"Error: status {resp.status_code}")


def get_properties(resp):
    return resp.json()["data"]["home_search"]["properties"]


def get_count(resp):
    return resp.json()["data"]["home_search"]["count"]


def save_results(properties, path):
    with open(path, "w") as fi:
        json.dump(properties, fi)


if __name__ == "__main__":
    main()
