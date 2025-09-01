import csv
import glob
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
from urllib.error import URLError
from urllib.robotparser import RobotFileParser
from urllib.parse import urljoin
from zipfile import ZipFile, ZIP_BZIP2

from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from requests.packages.urllib3.util.retry import Retry

from tor import TorSession

CONFIG_DIR = os.path.join(str(Path.home()), ".browsing")
DEFAULT_CONFIG = os.path.join(CONFIG_DIR, "browser.conf")
MAX_TOR_REQ = 50
MAX_ARCH_SIZE = 100 * 1000 * 1000


def cut_url(url):
    """
    If URL is longer than 50 characters, show the last 45.
    Useful for logging.

    :param str url:
    :return str: short URL
    """
    if len(url) > 50:
        return f"...{url[-45:]}"
    return url


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


class Browser:
    def __init__(
        self,
        base_url,
        stop_test=None,
        get_browsable=None,
        get_parsable=None,
        get_page_id=None,
        headers=None,
        user_agents=None,
        proxies=None,
        timeout=5,
        max_retries=5,
        backoff_factor=0.3,
        retry_on=(500, 502, 503, 504),
        browse_delay=0,
        html_parser="html.parser",
        soup_parser=None,
        config=DEFAULT_CONFIG,
    ):
        """
        Automated web browser.

        Notes about implementation:

        This class browses pages  within the same domain.

        Maintains two queues:
            - pages to browse
            - pages to parse (i.e. extradata from HTML code)

        This is useful when we browse through pages containing lists of links
        we want to parse, but the parsed links do not contain data indicating
        where we should browse next.

        :param str base_url: URL where to start browsing
        :param callable stop_test: function to determine if we should stop
                                   browsing
        :param callable get_browsable: function which returns the URL
                                       of the next page to browse
        :param callable get_parsable: function which returns the URL
                                      of the next page to parse
        :param callable get_page_id: function which shortens the URL into a
                                     unique ID
        :param dict headers: request headers (excluding user agent)
        :param list user_agents: request user agents
        :param list[dict] proxies: list of proxies dictionaries with format
                                   {procotol: ip:port}
        :param int timeout: timeout for requests
        :param int max_retries: maximum number of retries on request failure
        :param float backoff_factor: delay backoff factor for request retries
        :param list[int] retry_on: HTTP status codes allowing request retry
        :param float browse_delay: time in seconds to wait between page
                                   downloads
        :param str html_parser: parser to use with BeautifulSoup, e.g.
                                'html.parser', 'lxml', etc
        :param soup_parser: function to use to parse the HTML tags soup into
                            a dictionary
        :param str config: path to the browser configuration file
        """
        self.base_url = base_url
        self.stop_test = stop_test
        self.get_browsable = get_browsable
        self.get_parsable = get_parsable
        self.get_page_id = get_page_id
        self.headers = headers
        self.user_agents = user_agents
        self.proxies = proxies
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_on = retry_on
        self.soup_parser = soup_parser
        self.tor_session = None
        self.archive_count = 1

        if not html_parser:
            self.html_parser = partial(BeautifulSoup, features="html.parser")
        else:
            self.html_parser = partial(BeautifulSoup, features=html_parser)

        # parse the robots.txt file
        try:
            self.robot_parser = RobotFileParser()
            self.robot_parser.set_url(urljoin(base_url, "robots.txt"))
            self.robot_parser.read()
            if self.robot_parser.crawl_delay(""):
                self.browse_delay = self.robot_parser.crawl_delay("")
            else:
                self.browse_delay = browse_delay
        except URLError:
            logging.warning(
                "could not find robots.txt, setting craw_delay "
                f"to {browse_delay} seconds")
            self.browse_delay = browse_delay

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
        self.to_browse = deque()
        self.to_parse = deque()

    def configure(self, config_file):
        """
        Read and apply the configuration.

        Note: the implementation of this function is still in progress.

        :param str config_file: path to the configuration file
        """
        config = ConfigParser()
        config.read(config_file)
        self.harvest_dir = config["harvest"]["harvest_dir"]
        self.extract_csv = config["extract"]["csv_path"]

        self.csv_header = [
            col.strip() for col in config["extract"]["csv_header"].split(",")
        ]

        logging.basicConfig(
            format="%(asctime)s %(levelname)s %(message)s",
            level=logging.INFO,
            filename=config["logging"]["log_file"],
            filemode="a")

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
        self.headers["User-Agent"] = random.choice(self.user_agents)

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

    def can_fetch(self, agent, url):
        """
        Checks the robots.txt file if we can fetch the page.
        Always returns True if the website does not have a robots.txt file.

        :param str agent: user agent used for browsing
        :param str url: url to check
        :return bool: True if we can browse the page else False
        """
        if not url.startswith(self.base_url):
            url = urljoin(self.base_url, url)
        if self.robot_parser:
            return self.robot_parser.can_fetch(agent, url)
        return True

    def get_archive_name(self):
        """
        Returns the current harvest archive's name. Start with a suffix
        of '1' and increment when the archive's size > 100MB.

        :return str: harvest archive name
        """
        archive_name = os.path.join(
            self.harvest_dir,
            f"harvest_{self.archive_count}.bz2"
        )

        if os.path.exists(archive_name):
            if os.path.getsize(archive_name) > MAX_ARCH_SIZE:
                self.archive_acount += 1

        return os.path.join(
            self.harvest_dir,
            f"harvest_{self.archive_count}.bz2"
        )

    def get_session(
        self,
        max_retries,
        backoff_factor,
        retry_on,
    ):
        # new IP only after reset_identity() is called and new session is made
        session = TorSession(password=os.getenv("TOR_PASSWORD"))
        session.reset_identity()
        session = TorSession(password=os.getenv("TOR_PASSWORD"))

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
        timeout=None,
    ):
        """
        Download a web page using a Tor session.
        """
        # try to use self.tor_session, if self.tor_session does not exist
        # get a tor session and save it in self.tor_session
        if not self.tor_session or self.tor_session.used > MAX_TOR_REQ:
            self.tor_session = self.get_session(
                max_retries=self.max_retries,
                backoff_factor=self.backoff_factor,
                retry_on=self.retry_on,
            )
            self.headers = self.choose_headers()

        if not url.startswith(self.base_url):
            url = urljoin(self.base_url, url)

        try:
            response = self.tor_session.get(
                url,
                headers=self.headers,
                timeout=timeout,
            )
            return response.content

        except RequestException:
            logging.error(f"failed to download {cut_url(url)}")
            return

    def browse(self, initial=None):
        """
        Crawl the web in a breadth-first search fashion using Tor.

        :param str initial: URL where to start browsing (suffix to append
                            to the base URL)
        """
        logging.info("start browsing")
        if not initial:
            initial = self.base_url

        self.to_browse.appendleft(initial)
        self.explored.add(initial)

        while self.to_browse:
            current = self.to_browse.pop()
            if not self.can_fetch(self.headers["User-Agent"], current):
                logging.info(f"forbidden: {cut_url(current)}")
                continue

            logging.info(f"downloading {cut_url(current)}")
            content = self.download_page(url=current, timeout=self.timeout)
            time.sleep(self.browse_delay)

            # if download failed, push URL back to queue and
            # remove proxy from list
            if content is None:
                logging.info(
                    "pushing URL back into queue, getting new Tor session"
                )
                self.to_browse.appendleft(current)
                self.tor_session = self.get_session(
                    max_retries=self.max_retries,
                    backoff_factor=self.backoff_factor,
                    retry_on=self.retry_on,
                )
                self.headers = self.choose_headers()
                continue

            logging.info("parsing page")
            soup = self.html_parser(content)

            # get list of links to home details
            for child in self.get_parsable(soup):
                logging.info(f"found to parse: {cut_url(child)}")
                if self.explored.contains(child):
                    continue
                self.explored.add(child)
                self.to_parse.append(child)

            # check if we're at the last page
            # if yes return, else get next page of listings
            if self.stop_test(soup):
                logging.info("reached last page to browse, stopping")
                return

            for child in self.get_browsable(current):
                logging.info(f"found to browse next {cut_url(child)}")
                if self.explored.contains(child):
                    continue
                self.explored.add(child)
                self.to_browse.append(child)

    def harvest(self):
        """
        Download the web pages stored in self.to_parse using Tor.
        """
        logging.info("start harvesting")
        while self.to_parse:
            current = self.to_parse.pop()
            if not self.can_fetch(self.headers["User-Agent"], current):
                logging.info(f"forbidden: {cut_url(current)}")
                continue

            logging.info(f"downloading {cut_url(current)}")
            content = self.download_page(url=current, timeout=self.timeout)
            time.sleep(self.browse_delay)

            # if download failed, push URL back to queue and
            # remove proxy from list
            if content is None:
                logging.info(
                    "pushing URL back to queue, getting new Tor session"
                )
                self.to_parse.appendleft(current)
                self.tor_session = self.get_session(
                    max_retries=self.max_retries,
                    backoff_factor=self.backoff_factor,
                    retry_on=self.retry_on,
                )
                self.headers = self.choose_headers()
                continue

            logging.info(f"archiving {cut_url(current)}")
            file_name = self.get_page_id(current)
            archive_name = self.get_archive_name()
            with ZipFile(archive_name, "a", compression=ZIP_BZIP2) as archive:
                archive.writestr(file_name, content)

    def extract(self, csv_name=None, csv_header=None):
        """
        Extract data from HTML pages stored in an archive and saves it as a
        CSV file.

        :param str csv_name: name of the CSV file where to store data
        :param list[str] csv_header: list of column names for the CSV file
        """
        if not csv_name:
            csv_name = self.extract_csv
        if not csv_header:
            csv_header = self.csv_header

        for archive_name in glob.glob("harvest_[0-9]*.bz2"):
            with ZipFile(archive_name, "r", compression=ZIP_BZIP2) as archive:
                with open(csv_name, "w") as csv_obj:
                    writer = csv.DictWriter(
                        csv_obj,
                        csv_header,
                        lineterminator=os.linesep)

                    writer.writeheader()

                    for name in archive.namelist():
                        logging.info(f"parsing {name}")
                        content = archive.read(name)
                        soup = self.html_parser(content)
                        parsed = self.soup_parser(soup)
                        writer.writerow(parsed)

                names = archive.namelist()
                with open(csv_name) as csv_obj:
                    reader = csv.reader(csv_obj)
                    columns = len(next(reader))
                    for index, row in enumerate(reader):
                        nulls = row.count("NULL")
                        if nulls / columns > 0.3:
                            logging.warning(
                                f"{nulls} null values in {names[index]}")
