import json

import requests

from utils import make_headers, get_cookies

URL = "https://www.realtor.com/api/v1/rdc_search_srp?client_id=rdc-search-for-sale-search&schema=vesta"


# Cookie not required
# Content-Length not required
# newrelic not required
def update_headers(cookies):
    headers = make_headers("headers2")
    headers["Cookie"] = "; ".join(
        f"{co['name']}={co['value']}" for co in cookies
    )
    return headers


# This is the payload as extracted from Firefox dev tools
# visitor_id not required
# isClient not required
# search_promotion not required
# 200 results seems to be the max we can fetch, use offset
# get number of bathrooms with 'baths'
# get number of 1/2 bathrooms with 'half_baths'
with open("data2.json.bak") as fi:
    payload = json.load(fi)


def build_payload(cookies):
    with open("query.graphql") as fi:
        query = fi.read()

    with open("variables.json") as fi:
        variables = json.load(fi)

    # make sure visitor_id matches cookies
    for co in cookies:
        if co["name"] == "__vst":
            variables["visitor_id"] = co["value"]

    return {"query": query, **variables}


def make_request(headers, payload):
    return requests.post(
        URL,
        json=payload,
        headers=headers,
    )


def get_props(resp):
    return resp.json()["data"]["home_search"]["properties"]


if __name__ == "__main__":
    cookies = get_cookies()
    headers = update_headers(cookies)
    payload = build_payload(cookies)
    resp = make_request(headers, payload)
    resp.raise_for_status()
    props = get_props(resp)
