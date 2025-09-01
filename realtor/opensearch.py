import json
import os

import requests

import constants as cst

BASE_URL = os.getenv("OPENSEARCH_URL")
AUTH = (os.getenv("OPENSEARCH_USER"), os.getenv("OPENSEARCH_PW"))
HEADERS = {"Content-Type": "application/json"}
REQ_PARAMS = {
    "auth": AUTH,
    "headers": HEADERS,
}
DATA_DIR = "es_data"
HOMES_IDX = "homes"


def store_doc(index, _id, data):
    resp = requests.put(
        f"{BASE_URL}/{index}/_doc/{_id}",
        json=data,
        auth=AUTH,
        headers=HEADERS,
    )
    resp.raise_for_status()
    return resp.json()


def count_docs(index):
    resp = requests.get(
        f"{BASE_URL}/{index}/_count", auth=AUTH, headers=HEADERS,
    )
    resp.raise_for_status()
    return resp.json()


def store_docs():
    for file in os.listdir(DATA_DIR):
        path = os.path.join(DATA_DIR, file)
        file_id = os.path.splitext(file)[0]
        with open(path) as fi:
            data = json.load(fi)
        store_doc(HOMES_IDX, file_id, data)


def base_search(index, query, source, profile, human, size):
    full_query = {
        "_source": source,
        "query": query,
        "size": size,
        "profile": profile,
    }
    params = {"pretty": "true"}
    if human:
        params["human"] = "true"
    resp = requests.get(
        f"{BASE_URL}/{index}/_search",
        json=full_query,
        auth=AUTH,
        headers=HEADERS,
        params=params,
    )
    resp.raise_for_status()
    return resp.json()


def search_word(
    index, field, word, source=False, profile=False, human=False, size=3
):
    query = {
        "match": {field: {"query": word}},
    }
    return base_search(index, query, source, profile, human, size)


def search_words_and(
    index, field, words, source=False, profile=False, human=False, size=3
):
    query = {"match": {"description": {"query": words, "operator": "and"}}}
    return base_search(index, query, source, profile, human, size)


def search_multi_fields(
    index,
    fields,
    words,
    boost=None,
    source=False,
    profile=False,
    human=False,
    size=3,
):
    boost = boost or []
    boosted_fields = []
    for idx, fi in enumerate(fields):
        if idx < len(boost):
            boosted_fields.append(f"{fi}^{boost[idx]}")
        else:
            boosted_fields.append(fi)
    query = {
        "multi_match": {"query": words, "fields": boosted_fields,},
    }
    return base_search(index, query, source, profile, human, size)


def search_phrase(
    phrase,
    field,
    slop=0,
    index=HOMES_IDX,
    source=False,
    profile=False,
    human=False,
    size=3,
):
    query = {
        "match_phrase": {field: {"query": phrase, "slop": slop,}},
    }
    return base_search(index, query, source, profile, human, size)


def search_words_fuzzy(
    words,
    field,
    fuzzy=0,
    index=HOMES_IDX,
    source=False,
    profile=False,
    human=False,
    size=3,
):
    query = {
        "match": {field: {"query": words, "fuzziness": fuzzy,}},
    }
    return base_search(index, query, source, profile, human, size)


def term_query(
    field,
    value,
    index=HOMES_IDX,
    source=False,
    profile=False,
    human=False,
    size=3,
):
    query = {"term": {field: {"value": value}}}
    return base_search(index, query, source, profile, human, size)


def range_query(
    field,
    gte,
    lte,
    index=HOMES_IDX,
    source=False,
    profile=False,
    human=False,
    size=3,
):
    query = {
        "range": {field: {"gte": gte, "lte": lte,}},
    }
    return base_search(index, query, source, profile, human, size)


def bool_query():
    query = {
        "_source": True,
        "profile": True,
        "query": {
            "bool": {
                "must": [
                    {"match": {"description": "fireplace"}},
                    {"match_phrase": {"description": "new garbage disposal"}},
                ],
                "must_not": [{"range": {"beds": {"lt": 2}}}],
                "should": [{"term": {"baths": 2}}],
            }
        },
    }
    resp = requests.get(
        f"{BASE_URL}/{HOMES_IDX}/_search?pretty=true&human=true",
        json=query,
        auth=AUTH,
        headers=HEADERS,
    )
    resp.raise_for_status()
    return resp.json()


def avg(field):
    query = {
        "profile": True,
        "_source": False,
        "aggs": {f"avg_of_{field}": {"avg": {"field": field}}},
    }
    resp = requests.get(
        f"{BASE_URL}/{HOMES_IDX}/_search?pretty=true&human=true",
        json=query,
        auth=AUTH,
        headers=HEADERS,
    )
    resp.raise_for_status()
    return resp.json()


def health():
    resp = requests.get(f"{BASE_URL}/_cat/health")
    resp.raise_for_status()
    return resp.json()


def prepare_bulk_data(date):
    with open(
        os.path.join(cst.DATA_DIR, f"properties-for-sale-{date}.json")
    ) as fi:
        for_sale = json.load(fi)

    with open(
        os.path.join(cst.DATA_DIR, f"properties-descriptions-{date}.json")
    ) as fi:
        descriptions = json.load(fi)

    bulk_data = {}
    for desc in descriptions:
        bulk_data[desc["property_id"]] = {
            "action": {
                "index": {"_index": HOMES_IDX, "_id": desc["property_id"]}
            },
            "data": desc,
        }

    for prop in for_sale:
        if prop["property_id"] in bulk_data:
            bulk_data[prop["property_id"]]["data"].update(
                {
                    "beds": prop["description"]["beds"],
                    "baths": prop["description"]["baths"],
                    "year_built": prop["description"]["year_built"],
                    "sqft": prop["description"]["sqft"],
                    "list_price": prop["list_price"],
                }
            )

    with open(
        os.path.join(DATA_DIR, f"bulk_load_descriptions-{date}.json"), "w"
    ) as fi:
        for _, value in bulk_data.items():
            fi.write(json.dumps(value["action"]) + "\n")
            fi.write(json.dumps(value["data"]) + "\n")


def store_property_descriptions(date, index=HOMES_IDX):
    with open(
        os.path.join(DATA_DIR, f"bulk_load_descriptions-{date}.json")
    ) as fi:
        data = fi.read()

    resp = requests.post(
        f"{BASE_URL}/{index}/_bulk", data=data, auth=AUTH, headers=HEADERS,
    )
    resp.raise_for_status()
    return resp.json()


if __name__ == "__main__":
    resp = avg("list_price")
    print(json.dumps(resp))
