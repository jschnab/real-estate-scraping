import json
import logging
import os
import time
from datetime import timedelta

from milliped.download_managers import ChromeDownloadManager
from milliped.utils import get_logger

import constants as cst

LOG_CONF = {
    "handlers": [
        {
            "handler": "stream",
            "format": "%(asctime)s %(levelname)s %(message)s",
            "level": logging.INFO,
        }
    ]
}
LOGGER = get_logger("test", **LOG_CONF)

OPTIONS = {
    "page_load_strategy": "eager",
    "args": [
        "headless",
        "user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "window-size=2560,1440",
        "start-maximized",
    ],
}


def make_headers(path):
    headers = {}
    with open(path) as fi:
        for line in fi:
            key, value = line.split(": ", 1)
            headers[key] = value.strip()
    return headers


def get_cookies(url=cst.COOKIES_URL, cookies_path=cst.COOKIES_PATH):
    # if we have stored cookies, reuse them if they are
    # more recent than 1 day
    if os.path.isfile(cookies_path):
        modif_time = os.path.getmtime(cookies_path)
        if timedelta(seconds=time.time() - modif_time) < timedelta(days=1):
            with open(cookies_path) as fi:
                return json.load(fi)

    dm = ChromeDownloadManager(
        base_url=cst.BASE_URL,
        options=OPTIONS,
        logger=LOGGER,
    )
    dm.get(
        url,
        store_cookies=True,
    )
    dm.close()

    # cache cookies for later
    with open(cookies_path, "w") as fi:
        json.dump(dm.cookies, fi)

    return dm.cookies
