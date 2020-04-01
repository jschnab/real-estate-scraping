import hashlib
import json
import logging
import os
import random
import time

from collections import deque
from configparser import ConfigParser
from functools import partial
from pathlib import Path
from urllib.robotparser import RobotFileParser
from urllib.parse import urljoin

import requests

from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from requests.packages.urllib3.util.retry import Retry
from urllib.error import URLError

CONFIG_DIR = os.path.join(str(Path.home()), ".scraping")
DEFAULT_CONFIG = os.path.join(CONFIG_DIR, "crawler.conf")

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)


class Explored:
    def __init__(self):
        self.explored = set()

    def add(self, *args):
        """
        Add args to the set.
        Note: args should be of type 'string'.
        """
        for a in args:
            if not isinstance(a, str):
                raise TypeError(f"Expected {a} to be str, got {type(a)}")
            self.explored.add(hashlib.md5(a.encode()).hexdigest())

    def contains(self, *args):
        """
        Check if the args are present in the set.
        Returns True if none of the args are in the set, else False.
        Note: args should be of type 'string'.
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
        timeout=5,
        max_retries=8,
        backoff_factor=0.3,
        retry_on=[500, 502, 503, 504],
        crawl_delay=0.5,
        html_parser=None,
        soup_parser=None,
        config=DEFAULT_CONFIG,
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
        :param html_parser: tool to use for parsing web page contents
        :param soup_parser: function to use to parse the HTML tags soup into
                            a dictionary
        :param str config: path to the crawler configuration file
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
        self.soup_parser = soup_parser

        if not html_parser:
            self.html_parser = partial(BeautifulSoup, features="html.parser")
        else:
            self.html_parser = html_parser

        # parse the robots.txt file
        try:
            self.robot_parser = RobotFileParser()
            self.robot_parser.set_url(urljoin(base_url, "robots.txt"))
            self.robot_parser.read()
            self.crawl_delay = self.robot_parser.crawl_delay("") or crawl_delay
        except URLError:
            logging.warning(
                "could not find robots.txt, setting craw_delay "
                f"to {crawl_delay} seconds")
            self.crawl_delay = crawl_delay

        # apply config
        if Path(config).exists():
            self.configure(config)
        if Path(os.path.join(CONFIG_DIR, "proxies")).exists():
            self.get_proxies(os.path.join(CONFIG_DIR, "proxies"))
        if Path(os.path.join(CONFIG_DIR, "headers")).exists():
            self.get_headers(os.path.join(CONFIG_DIR, "headers"))
        if Path(os.path.join(CONFIG_DIR, "user_agents")).exists():
            self.get_user_agents(os.path.join(CONFIG_DIR, "user_agents"))

        self.explored = Explored()
        self.to_crawl = deque()
        self.to_parse = deque()

    def configure(self, config_file):
        """
        Read and apply the configuration.

        Note: the implementation of this function is still in progress.

        :param str config_file: path to the configuration file
        """
        config = ConfigParser()
        config.read(config_file)

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

    def get_proxies(self, file_name, fmt=False):
        """
        Read and format request proxies from a JSON file.

        :param str file_name: name of the file storing proxies
        :return dict: request headers
        """
        with open(file_name) as f:
            self.proxies = json.load(f)

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

        except RequestException:
            logging.error(f"too many retries while downloading {url}")
            return

    def crawl(self, initial=None):
        """
        Crawl the web in a breadth-first search fashion.

        :param str initial: URL where to start crawling (suffix to append
                            to the base URL)
        """
        logging.info("start crawling")
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

            current = self.to_crawl.pop()
            if not self.can_crawl(headers["User-Agent"], current):
                logging.info("cannot crawl the current page, skipping")
                continue

            logging.info(f"downloading {current}")
            contents = self.download_page(
                url=current,
                headers=headers,
                proxies=proxy,
                timeout=self.timeout,
            )

            time.sleep(self.crawl_delay)

            # if download failed, push URL back to queue and
            # remove proxy from list
            if contents is None:
                logging.info(f"download {current} failed, will retry later")
                self.to_crawl.appendleft(current)
                self.proxies.remove(proxy)
                continue

            logging.info("parsing page")
            soup = self.parser(contents)

            # get list of links to home details
            for child in self.get_parsable(soup):
                logging.info(f"found {child} to parse")
                if self.explored.contains(child):
                    continue
                self.explored.add(child)
                self.to_parse.append(child)

            # check if we're at the last page
            # if yes return, else get next page of listings
            if self.stop_test(soup):
                logging.info("reached last page to crawl")
                return

            for child in self.get_crawlable(current):
                logging.info(f"found {child} to crawl next")
                if self.explored.contains(child):
                    continue
                self.explored.add(child)
                self.to_crawl.append(child)

        def harvest(self, archive_name):
            """
            Download the web pages stored in self.to_parse.

            :param str archive_name: path to the archive file where the
                                     web pages are stored after download
            """
            pass
