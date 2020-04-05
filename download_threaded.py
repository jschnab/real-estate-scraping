import json
import os
import time
import sys

from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from urllib.parse import urljoin

from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from tor import TorSession


def download_page(q, headers):
    session = TorSession(password=os.getenv("TOR_PASSWORD"))
    stat = (500, 502, 503, 504)
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=stat)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    while not q.empty():
        time.sleep(1)
        content = None
        suffix = q.get()
        url = urljoin("https://www.nytimes.com", suffix)
        try:
            content = session.get(url, timeout=5, headers=headers)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(e)
            q.put(suffix)
        if content:
            print(f"downloaded {suffix}")
        else:
            print(f"failed {suffix}")


if __name__ == "__main__":
    mode = sys.argv[1]

    # get headers
    with open("/home/jonathans/.browsing/headers") as f:
        headers = json.load(f)
    with open("/home/jonathans/.browsing/user_agents") as f:
        headers["User-Agent"] = json.load(f)[0]

    # make queue
    with open("/home/jonathans/real-estate-scraping/toparse.json") as f:
        toparse = json.load(f)
    q = Queue()
    for link in toparse:
        q.put(link)

    if mode == "simple":
        print("start harvest")
        start = time.time()
        download_page(q, headers)
        stop = time.time()
        elapsed = stop - start
        print(f"took {elapsed:.2f} seconds")
        sys.exit()

    if mode == "threaded":
        print("start harvest")
        start = time.time()
        with ThreadPoolExecutor(max_workers=24) as executor:
            executor.submit(download_page, q, headers)
        print("finished harvest")
        stop = time.time()
        elapsed = stop - start
        print(f"took {elapsed:.2f} seconds")
        sys.exit()
