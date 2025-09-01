import json
import sys

import requests

sys.path.insert(0, "..")

import utils

url = "https://www.zillow.com/async-create-search-page-state"

headers_for_sale = utils.make_headers("headers_for_sale")
headers_sold = utils.make_headers("headers_sold")

# example of pagination:
# {"currentPage": 2}
# {} for page 1
with open("data_for_sale.json") as fi:
    data_for_sale = json.load(fi)
with open("data_sold.json") as fi:
    data_sold = json.load(fi)


def make_request(headers, data):
    return requests.put(
        url,
        headers=headers,
        json=data,
    )


def get_props(response):
    return response.json()['cat1']['searchResults']['listResults']
