import json
import time

from concurrent.futures import ThreadPoolExecutor
from queue import Queue

import browser
import nytimes.browse
import nytimes.parse_soup


def download_page(q):
    while not q.empty():
        crawler = browser.Browser(
            "https://www.nytimes.com",
            stop_test=nytimes.browse.is_last_page,
            get_browsable=nytimes.browse.wrapper_next_page,
            get_parsable=nytimes.browse.get_listings,
            get_page_id=nytimes.browse.get_listing_id,
            soup_parser=nytimes.parse_soup.parse_webpage,
        )
        url = q.get()
        content = crawler.download_page_tor(url, timeout=5)
        if content:
            print(f"downloaded {url}")
        else:
            print(f"failed {url}")


if __name__ == "__main__":
    # make queue
    with open("/home/jonathans/real-estate-scraping/toparse.json") as f:
        toparse = json.load(f)
    q = Queue()
    for link in toparse:
        q.put(link)

    print("start harvest")
    start = time.time()
    with ThreadPoolExecutor(max_workers=24) as executor:
        executor.submit(download_page, q)
    print("finished harvest")
    stop = time.time()
    elapsed = stop - start
    print(f"took {elapsed:.2f} seconds")
