#!/usr/bin/env python3

import json
import os
from datetime import datetime

import requests

import constants as cst
import load
from download_description import get_description
from utils import LOGGER, make_headers, get_cookies

URL = (
    "https://www.realtor.com/api/v1/"
    "rdc_search_srp?client_id=rdc-search-for-sale-search&schema=vesta"
)
TEMP_FILE = "temp.json"
RESULTS_PATH = os.path.join(cst.DATA_DIR, "properties-{date}.json")

REQ_VARIABLES = {
    "variables": {
        "geoSupportedSlug": "Lansing_MI",
        "query": {
            "status": ["sold"],
            "search_location": {"location": "Lansing, MI"},
            "type": [
                "single_family",
                "condo",
                "condos",
                "condo_townhome_rowhome_coop",
                "condo_townhome",
                "townhomes",
                "duplex_triplex",
                "multi_family",
                "farm",
                "mobile",
                "land",
            ],
            "sold_date": {"min": "2023-09-21T01:45:06.380Z"},
        },
        "client_data": {"device_data": {"device_type": "desktop"}},
        "limit": 200,
        "offset": 0,
        "sort": [
            {"field": "sold_date", "direction": "desc"},
            {"field": "photo_count", "direction": "desc"},
        ],
    },
    "isClient": True,
    "visitor_id": "85826405-4fe7-44a0-a8ef-b4297a815904",
}


def main():
    LOGGER.info("Getting cookies")
    cookies = get_cookies()
    LOGGER.info("Got cookies")

    LOGGER.info("Preparing request headers")
    headers = make_headers("headers2")
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
    flag = True
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
        props = get_properties(resp)
        #LOGGER.info("Adding description text to each property")
        #if flag:
        #    flag = False
        #    add_descriptions(props, cookies)
        #LOGGER.info("Finished dding description text to each property")
        properties.extend(props)
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
            properties,
            results_path,
        )
        LOGGER.info("Saved results")

    if os.path.isfile(TEMP_FILE):
        LOGGER.info("Deleting temporary file")
        os.remove(TEMP_FILE)
        LOGGER.info("Deleted temporary file")

    LOGGER.info("Loading new properties to the database")
    load.insert_properties(results_path)

    LOGGER.info("Done")


def build_payload(cookies):
    with open("query.graphql") as fi:
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
        LOGGER.error("Error: status {resp.status_code}")


def get_properties(resp):
    return resp.json()["data"]["home_search"]["properties"]


def add_descriptions(properties, cookies):
    for prop in properties:
        print(get_description(prop["permalink"], cookies))
        break


def get_count(resp):
    return resp.json()["data"]["home_search"]["count"]


def save_results(properties, path):
    with open(path, "w") as fi:
        json.dump(properties, fi)


if __name__ == "__main__":
    main()
