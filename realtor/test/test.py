import json
import sys

import requests


with open("headers") as fi:
    data = fi.readlines()

headers = {}
for line in data:
    key, value = line.split(": ")
    headers[key] = value.strip()

with open("request.json") as fi:
    request = json.load(fi)


url = "https://www.realtor.com/api/v1/rdc_search_srp?client_id=rdc-search-new-communities&schema=vesta"


resp = requests.post(
    url,
    headers=headers,
    json=request,
)

resp.raise_for_status()

print(resp.json())
