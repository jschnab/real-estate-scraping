import json
import sys

import requests

sys.path.insert(0, "..")
from utils import make_headers

url = "https://www.realtor.com/api/v1/rdc_search_srp?client_id=rdc-search-new-communities&schema=vesta"

data = {"query":"\n  query TransformCommunitySearch($query: CommunitySearchCriteria!, $limit: Int) {\n    community_search(query: $query, limit: $limit) {\n      count\n      total\n      results {\n        source {\n          id\n        }\n        community_metrics {\n          leads_month_to_date\n        }\n        builder {\n          builder_id\n          href\n          name\n          source_builder_id\n          logo {\n            href\n          }\n        }\n        property_id\n        description {\n          name\n          baths_min\n          baths_max\n          beds_max\n          beds_min\n          sqft_max\n          sqft_min\n        }\n        location {\n          address {\n            city\n            state_code\n            postal_code\n          }\n        }\n        list_price_max\n        list_price_min\n        primary_photo(https:true) {\n          description\n          href\n        }\n        permalink\n      }\n    }\n  }\n","variables":{"query":{"sales_builder":True,"search_location":{"location":"Lansing, MI","buffer":20}},"limit":200},"nrQueryType":"PREMIUM_CARD_SRP","isClient":True,"visitor_id":"85826405-4fe7-44a0-a8ef-b4297a815904"}


def main():
    resp = requests.post(
        url,
        data=json.dumps(data),
        headers=make_headers("headers1"),
    )
    print("status:", resp.status_code)
    return resp


if __name__ == "__main__":
    resp = main()
