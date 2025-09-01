import json

import requests
from bs4 import BeautifulSoup


def find_request_containing_house_description():
    with open("snapshot.har") as fi:
        data = json.load(fi)

    for ent in data["log"]["entries"]:
        if ent["request"]["url"].endswith("from=srp-list-card"):
            return ent


def get_description():
    with open("temp.html") as fi:
        doc = fi.read()

    soup =  BeautifulSoup(doc, "html.parser")

    data = json.loads(soup.find(id="__NEXT_DATA__").text)

    return data["props"]["pageProps"]["initialReduxState"]["propertyDetails"]["description"]["text"]


if __name__ == "__main__":
    desc = get_description()
