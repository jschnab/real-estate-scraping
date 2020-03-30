import hashlib
import json
import logging
import random
import time

from collections import deque
from functools import partial
from urllib.robotparser import RobotFileParser
from urllib.parse import urljoin

import requests

from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from urllib.error import URLError
from urllib3.exceptions import MaxRetryError

from proxy_utils import format_proxies

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)


class Explored:
    def __init__(self):
        self.explored = set()

    def add(self, *args):
        """
        Add arguments to the set.
        Arguments should be of type 'string'.
        """
        for a in args:
            if not isinstance(a, str):
                raise TypeError(f"Expected {a} to be str, got {type(a)}")
            self.explored.add(hashlib.md5(a.encode()).hexdigest())

    def contains(self, *args):
        """
        Check if the arguments are present in the set.
        Returns True if none of the arguments are in the set, else False.
        Arguments should be of type 'string'.
        """
        for a in args:
            if not isinstance(a, str):
                raise TypeError(f"Expected {a} to be str, got {type(a)}")
            if hashlib.md5(a.encode()).hexdigest() in self.explored:
                return True
        return False


class Crawler:
    def __init__(
        self,
        base_url,
        stop_test=None,
        get_crawlable=None,
        get_parsable=None,
        headers=None,
        user_agents=None,
        proxies=None,
        timeout=10,
        max_retries=10,
        backoff_factor=0.3,
        retry_on=[500, 502, 503, 504],
        crawl_delay=0.5,
        parser=None,
    ):
        """
        Web crawler class.

        Notes about implementation:

        This class is for crawling within the same domain.

        Maintains two queues:
            - pages to crawl
            - pages to parse (i.e. extradata from HTML code)

        This is useful when we crawl through pages containing lists of links
        we want to parse, but the parsed links do not contain data indicating
        where we should crawl next.

        :param str base_url: URL where to start crawling
        :param callable stop_test: function to determine if we should stop
                                   crawling
        :param callable get_crawlable: function which returns the URL
                                       of the next page to crawl
        :param callable get_parsable: function which returns the URL
                                      of the next page to parse
        :param dict headers: request headers (excluding user agent)
        :param list user_agents: request user agents
        :param list[dict] proxies: list of proxies dictionaries with format
                                   {procotol: ip:port}
        :param int timeout: timeout for requests
        :param int max_retries: maximum number of retries on request failure
        :param float backoff_factor: delay backoff factor for request retries
        :param list[int] retry_on: HTTP status codes allowing request retry
        :param float crawl_delay: time in seconds to wait between page
                                  downloads
        :param parser: class to use for parsing web page contents
        """
        self.base_url = base_url
        self.stop_test = stop_test
        self.get_crawlable = get_crawlable
        self.get_parsable = get_parsable
        self.headers = headers
        self.user_agents = user_agents
        self.proxies = proxies
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_on = retry_on

        if not parser:
            self.parser = partial(BeautifulSoup, features="html.parser")
        else:
            self.parser = parser

        # parse the robots.txt file
        try:
            self.robot_parser = RobotFileParser()
            self.robot_parser.set_url(urljoin(base_url, "robots.txt"))
            self.robot_parser.read()
            self.crawl_delay = self.robot_parser.crawl_delay or crawl_delay
        except URLError:
            logging.warning(
                "could not find robots.txt, setting craw_delay "
                f"to {crawl_delay} seconds")
            self.crawl_delay = crawl_delay

        self.explored = Explored()
        self.to_crawl = deque()
        self.to_parse = deque()

    def get_headers(self, file_name):
        """
        Read request headers (excluding user agent) from a JSON file.

        :param str file_name: name of the file storing headers
        :return dict: request headers
        """
        with open(file_name) as f:
            self.headers = json.load(f)

    def get_user_agents(self, file_name):
        """
        Read request user agents from a JSON file.

        :param str file_name: name of the file storing user agents
        :return dict: request headers
        """
        with open(file_name) as f:
            self.user_agents = json.load(f)

    def get_proxies(self, file_name):
        """
        Read and format request proxies from a JSON file.

        :param str file_name: name of the file storing proxies
        :return dict: request headers
        """
        with open(file_name) as f:
            self.proxies = format_proxies(json.load(f))

    def choose_headers(self):
        """
        Generate full request headers by randomly selecting a user agent
        from the list and adding it to headers.
        If user agents are not used, return basic headers.

        :return dict: request headers
        """
        headers = self.headers
        if self.user_agents:
            headers["User-Agent"] = random.choice(self.user_agents)
        return headers

    def choose_proxy(self):
        """
        Randomly select a proxy from the list.
        If proxies are not used, return None.

        :return dict: request proxy
        """
        if self.proxies is None:
            return
        return random.choice(self.proxies)

    def can_crawl(self, agent, url):
        """
        Checks the robots.txt file if we can crawl the page.
        Always returns True if the website does not have a robots.txt file.

        :param str agent: user agent used for crawling
        :param str url: url to check
        :return bool: True if we can crawl the page else False
        """
        if self.robot_parser:
            return self.robot_parser.can_fetch(agent, url)
        return True

    def get_session(
        self,
        max_retries,
        backoff_factor,
        retry_on,
    ):
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

    def download_page(
        self,
        url,
        session=None,
        headers=None,
        proxies=None,
        timeout=None,
    ):
        if not session:
            session = self.get_session(
                max_retries=self.max_retries,
                backoff_factor=self.backoff_factor,
                retry_on=self.retry_on,
            )

        if not url.startswith(self.base_url):
            url = urljoin(self.base_url, url)

        try:
            response = session.get(
                url,
                headers=headers,
                proxies=proxies,
                timeout=timeout,
            )
            return response.content

        except MaxRetryError:
            return

    def crawl_bfs(self, initial=None):
        """
        Crawl the web in a breadth-first search fashion.

        :param str initial: URL where to start crawling (suffix to append
                            to the base URL)
        """
        if not initial:
            initial = self.base_url

        if not initial.startswith(self.base_url):
            initial = urljoin(self.base_url, initial)

        self.to_crawl.appendleft(initial)
        self.explored.add(initial)

        while self.to_crawl:
            headers = self.choose_headers()
            try:
                proxy = self.choose_proxy()
            except IndexError:
                logging.error("all proxies have been exhausted, stopping")
                return

            crawl_current = self.to_crawl.pop()
            if not self.can_crawl(headers["User-Agent"], crawl_current):
                continue

            contents_current = self.download_page(
                url=crawl_current,
                headers=headers,
                proxies=proxy,
                timeout=self.timeout,
            )

            time.sleep(self.crawl_delay)

            # if download failed, push URL back to queue and
            # remove proxy from list
            if contents_current is None:
                self.to_crawl.appendleft(crawl_current)
                self.proxies.remove(proxy)
                continue

            soup = self.parser(contents_current)

            # get list of links to home details
            for child in self.get_parsable(soup):
                if self.explored.contains(child):
                    continue
                self.explored.add(child)
                self.to_parse.append(child)

            # check if we're at the last page
            # if yes return, else get next page of listings
            if self.stop_test(soup):
                return

            for child in self.get_crawlable(crawl_current):
                if self.explored.contains(child):
                    continue
                self.explored.add(child)
                self.to_crawl.append(child)
