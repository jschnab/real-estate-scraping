#!/usr/bin/env python3

import json
import sys
from datetime import datetime

import matplotlib.colors as mcolors
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import requests

sys.path.insert(0, "..")
import utils

URL = "https://api.frontdoor.realtor.com/graphql"

VARIABLES = {
    "operationName": "DPPropertyEstimates",
    "variables": {
        "historicalYearsMin": "2019-03-01",
        "historicalYearsMax": "2024-03-01",
        "forecastedMonthsMax":"2024-06-01",
    },
}

DATE_FORMAT = "%Y-%m-%d"

COLORS = list(mcolors.TABLEAU_COLORS.keys())


def make_request(headers, payload):
    return requests.post(
        URL,
        json=payload,
        headers=headers,
    )


def get_estimates(resp):
    return resp.json()["data"]["home"]["estimates"]


def parse_date(date_str, date_fmt=DATE_FORMAT):
    return datetime.strptime(date_str, date_fmt)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <property-id>")
        sys.exit(1)
    headers = utils.make_headers("headers")
    with open("query.graphql") as fi:
        query = fi.read()
    variables = VARIABLES.copy()
    variables["variables"]["propertyId"] = sys.argv[1]
    payload = {"query": query, **variables}
    resp = make_request(headers, payload)
    resp.raise_for_status()
    estim = get_estimates(resp)
    if estim["current_values"] is not None:
        print(estim["current_values"])

    historical = {}
    for hist in estim["historical_values"]:
        name = hist["source"]["name"]
        estimates = sorted(hist["estimates"], key=lambda x: x["date"])
        historical[name] = estimates

    forecast = {}
    for fc in estim["forecast_values"]:
        name = fc["source"]["name"]
        estimates = sorted(fc["estimates"], key=lambda x: x["date"])
        forecast[name] = estimates

    fig, ax = plt.subplots(figsize=(12, 8))
    for i, (name, values) in enumerate(historical.items()):
        ax.plot(
            [parse_date(val["date"]) for val in values[-10:]],
            [val["estimate"] for val in values[-10:]],
            label=f"{name} history",
            color=COLORS[i],
        )
        if name in forecast:
            ax.plot(
                [parse_date(val["date"]) for val in forecast[name]],
                [val["estimate"] for val in forecast[name]],
                label=f"{name} forecast",
                color=COLORS[i],
                linestyle="--",
            )
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%Y"))
    fig.autofmt_xdate()
    ax.grid(linestyle=":")
    fig.legend()
    plt.show()
