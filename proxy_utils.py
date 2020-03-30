import logging

import requests

from lxml import html
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)

PROXIES_PROVIDER = "https://free-proxy-list.net"
VALIDATE_URL = "https://www.google.com"


def download_proxies(url=PROXIES_PROVIDER):
    response = requests.get(url)
    response.raise_for_status()
    return response.content


def format_col_name(col_name):
    return col_name.lower().replace(" ", "_")


def get_proxies_from_page(page):
    doc = html.fromstring(page)
    rows = doc.xpath("//tr")
    columns = [format_col_name(row.text_content()) for row in rows[0]]
    proxies = []
    for row in rows[1:300]:
        proxies.append(
            dict(zip(columns, [field.text_content() for field in row]))
        )
    return proxies


def filter_proxies(proxies):
    filtered = []
    for p in proxies:
        if p["anonymity"] in ("anonymous", "elite proxy"):
            if p["https"] == "yes":
                filtered.append(p)
    return filtered


def format_proxies(proxies):
    formatted = []
    for p in proxies:
        if p["https"] == "yes":
            protocol = "https"
        else:
            protocol = "http"
        address = f"{p['ip_address']}"
        port = f"{p['port']}"
        formatted.append({protocol: f"{protocol}://{address}:{port}"})
    return formatted


def validate_proxies(
    proxies,
    url=VALIDATE_URL,
    timeout=10,
    max_retries=4,
    backoff_factor=0.3,
    retry_on=[500, 502, 503, 504],
):
    # prepare requests session
    session = requests.Session()
    retry = Retry(
        total=max_retries,
        read=max_retries,
        connect=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=retry_on,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

    # validate proxies by testing a request through them
    validated = []
    formatted = format_proxies(proxies)
    for i, p in enumerate(formatted):
        try:
            r = session.get(url, proxies=p, timeout=timeout)
            if r.ok:
                validated.append(proxies[i])
                logging.info(f"{i+1}/{len(formatted)} {p['https']} OK")
            else:
                logging.info(f"{i+1}/{len(formatted)} {p['https']} BAD")
        except Exception:
            logging.info(f"{i+1}/{len(formatted)} {p['https']} BAD")

    return validated


def collect_proxies(anonymous=True, validate=False, fmt=True):
    page = download_proxies()
    proxies = get_proxies_from_page(page)
    if anonymous:
        proxies = filter_proxies(proxies)
    if validate:
        proxies = validate_proxies(proxies)
    if fmt:
        proxies = format_proxies(proxies)
    return proxies
