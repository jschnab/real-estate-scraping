import bz2
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
from datetime import datetime
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.error import URLError
from urllib.robotparser import RobotFileParser
from urllib.parse import urljoin, urlparse

import boto3

from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from requests.packages.urllib3.util.retry import Retry

import aws_utils

from tor import TorSession

CONFIG_DIR = os.path.join(str(Path.home()), ".browsing")
MAX_TOR_REQ = 50
HARVEST_PAUSE_BACKOFF = 0.3
HARVEST_PAUSE_MAX = 60 * 30  # 30 minutes


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
        timeout=5,
        max_retries=5,
        backoff_factor=0.3,
        retry_on=(500, 502, 503, 504),
        browse_delay=0,
        html_parser="html.parser",
        soup_parser=None,
        geolocator=None,
        config_file="browser.conf",
        harvest_date=None,
    ):
        """
        Automated web browser.

        Notes about implementation:

        This class browses pages  within the same domain.

        Uses two queues:
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
        :param int timeout: timeout for requests
        :param int max_retries: maximum number of retries on request failure
        :param float backoff_factor: delay backoff factor for request retries
        :param list[int] retry_on: HTTP status codes allowing request retry
        :param float browse_delay: time in seconds to wait between page
                                   downloads
        :param str html_parser: parser to use with BeautifulSoup, e.g.
                                'html.parser', 'lxml', etc
        :param callable soup_parser: function to use to parse the HTML tags
                                     soup into a dictionary
        :param callable geolocator: function which adds latitude and longitude
                                    to a home listing
        :param str config_file: name of the configuration file
        :param str harvest_date: date of harvest, format YYYYMMDD
        """
        self.base_url = base_url
        self.stop_test = stop_test
        self.get_browsable = get_browsable
        self.get_parsable = get_parsable
        self.get_page_id = get_page_id
        self.headers = headers
        self.user_agents = user_agents
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_on = retry_on
        self.soup_parser = soup_parser
        self.session = None
        self.to_browse = deque()
        self.sqs_client = boto3.client("sqs")
        self.s3_client = boto3.client("s3")
        self.explored = Explored()
        self.harvest_pauses = 0
        self.harvest_date = self.set_harvest_date(harvest_date)
        if not html_parser:
            self.html_parser = partial(BeautifulSoup, features="html.parser")
        else:
            self.html_parser = partial(BeautifulSoup, features=html_parser)
        self.geolocator = geolocator

        # parse the robots.txt file
        try:
            self.robot_parser = RobotFileParser()
            self.robot_parser.set_url(urljoin(self.base_url, "robots.txt"))
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
        self.config_path = os.path.join(CONFIG_DIR, config_file)
        if Path(self.config_path).exists():
            self.configure(self.config_path)

    def set_harvest_date(self, date):
        """
        Set the harvest date.

        :param str date: format YYYYMMDD, or None to set to today's date
        :return str: format YYYY/MM/DD
        """
        if not date:
            return datetime.utcnow().strftime("%Y/%m/%d")
        return datetime.strptime(date, "%Y%m%d").strftime("%Y/%m/%d")

    def configure(self, config_file):
        """
        Read and apply the configuration.

        Note: the implementation of this function is still in progress.

        :param str config_file: path to the configuration file
        """
        self.config = ConfigParser()
        self.config.read(config_file)
        conf = self.config
        self.sqs_queue = conf["sqs"]["queue_url"]
        self.s3_bucket = conf["s3"]["bucket"]
        self.harvest_key_prefix = conf["harvest"]["key_prefix"]
        self.extract_key_prefix = conf["extract"]["key_prefix"]
        self.geoloc_key_prefix = conf["geolocation"]["key_prefix"]
        self.extract_csv_header = conf["extract"]["csv_header"].split(",")
        self.geoloc_csv_header = conf["geolocation"]["csv_header"].split(",")
        self.get_headers(os.path.join(CONFIG_DIR, "headers"))
        self.get_user_agents(os.path.join(CONFIG_DIR, "user_agents"))

        logging.basicConfig(
            format="%(asctime)s %(levelname)s %(message)s",
            level=logging.INFO,
            filename=os.path.join(CONFIG_DIR, "browser.log"),
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

    def push_queue(self, url):
        """
        Push URL into an SQS queue.

        :param str url: URL to push to add to the queue
        """
        self.sqs_client.send_message(
            QueueUrl=self.sqs_queue,
            MessageBody=url,
        )

    def pop_queue(self):
        """
        Pop URL from an SQS FIFO queue.

        :return str: receipt handle of the message (for deletion of the
                     message)
        """
        response = self.sqs_client.receive_message(
            QueueUrl=self.sqs_queue, WaitTimeSeconds=20)
        if response:
            messages = response.get("Messages")
            if messages:
                # we only receive one message at a time
                handle = messages[0].get("ReceiptHandle")
                body = messages[0].get("Body")
                return handle, body
        return None, None

    def delete_message(self, receipt_handle):
        """
        Delete a processed message from an SQS FIFO queue.

        :param str receipt_handle: from receive_message()
        """
        try:
            self.sqs_client.delete_message(
                QueueUrl=self.sqs_queue,
                ReceiptHandle=receipt_handle,
            )
        except Exception as e:
            logging.info(
                f"failed to delete msg with handle '{receipt_handle}' "
                f"error: {e}"
            )

    def pause_harvest(self):
        """
        Pause during harvest if no message is returned from the queue.
        Pause times increase exponentially until a defined maximum is reached.
        """
        duration = min(
            HARVEST_PAUSE_BACKOFF * 2 ** self.harvest_pauses,
            HARVEST_PAUSE_MAX
        )
        time.sleep(duration)
        self.harvest_pauses += 1

    def store_harvest(self, file_prefix, data):
        """
        Stores the data from a web page in a bz2 file and store it in
        AWS S3.

        :param str file_prefix: name of the compressed file without extension
        :param bytes data: data to store
        """
        compressed = bz2.compress(data)
        k = f"{self.harvest_key_prefix}/{self.harvest_date}/{file_prefix}.bz2"
        self.s3_client.put_object(
            Body=compressed,
            Bucket=self.s3_bucket,
            Key=k,
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
        # try to use self.session, if self.session does not exist
        # get a tor session and save it in self.session
        if not self.session or self.session.used > MAX_TOR_REQ:
            logging.info("getting a new Tor session")
            self.session = self.get_session(
                max_retries=self.max_retries,
                backoff_factor=self.backoff_factor,
                retry_on=self.retry_on,
            )
            self.headers = self.choose_headers()

        if not url.startswith(self.base_url):
            url = urljoin(self.base_url, url)

        try:
            response = self.session.get(
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
                self.session = self.get_session(
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
                self.push_queue(child)

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
        Download the web pages stored in an SQS queue using Tor.
        """
        logging.info("start harvesting")
        while True:
            handle, current = self.pop_queue()
            if not current:
                logging.info("no message received, pausing")
                self.pause_harvest()
                continue
            self.harvest_pauses = 0
            if not self.can_fetch(self.headers["User-Agent"], current):
                logging.info(f"forbidden: {cut_url(current)}")
                self.delete_message(handle)
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
                self.delete_message(handle)
                self.push_queue(current)
                self.session = self.get_session(
                    max_retries=self.max_retries,
                    backoff_factor=self.backoff_factor,
                    retry_on=self.retry_on,
                )
                self.headers = self.choose_headers()
                continue

            file_prefix = self.get_page_id(current)
            logging.info(f"archiving {file_prefix}")
            self.store_harvest(file_prefix, content)
            self.delete_message(handle)

    def extract(self):
        """
        Parse HTML code from web pages to extract information and store as a
        CSV file.
        HTML is processed according to the function passed in 'html_parser' and
        data is extracted according to the function passed in 'soup_parser'.
        """
        with TemporaryDirectory() as temp_dir:
            logging.info(f"downloading files to {temp_dir}")
            aws_utils.download_files(
                self.s3_bucket,
                f"{self.harvest_key_prefix}/{self.harvest_date}",
                temp_dir,
            )

            csv_path = os.path.join(temp_dir, "extract.csv")
            with open(csv_path, "w") as csv_file:
                writer = csv.DictWriter(
                    csv_file,
                    self.extract_csv_header,
                    lineterminator=os.linesep,
                )
                writer.writeheader()

                # iterate over HTML documents, extract data and write to CSV
                file_names = []
                for f in glob.glob(f"{temp_dir}/*.bz2"):
                    logging.info(f"parsing {f}")
                    listing_id = os.path.split(os.path.splitext(f)[0])[-1]
                    file_names += listing_id,
                    with bz2.open(f, "rb") as zip_file:
                        writer.writerow({
                            **self.soup_parser(
                                self.html_parser(zip_file.read())
                            ),
                            "listing_id": listing_id,
                            "source": urlparse(self.base_url).netloc,
                            "collection_date": self.harvest_date,
                        })

            # check how fields are empty on each row
            with open(csv_path) as csv_file:
                reader = csv.reader(csv_file)
                columns = len(next(reader))
                for index, row in enumerate(reader):
                    nulls = row.count("NULL")
                    if nulls / columns > 0.3:
                        logging.warning(
                            f"{nulls} null values in {file_names[index]}"
                        )

            # upload CSV file to S3
            csv_s3_key = (
                f"{self.extract_key_prefix}/{self.harvest_date}/extract.csv"
            )
            logging.info(f"uploading data to {self.s3_bucket}/{csv_s3_key}")
            client = boto3.client("s3")
            client.upload_file(
                csv_path,
                self.s3_bucket,
                csv_s3_key,
            )

        logging.info("extraction finished")

    def geolocalize(self):
        logging.info("starting geolocation")

        with TemporaryDirectory() as temp_dir:
            input_csv_s3_key = (
                f"{self.extract_key_prefix}/{self.harvest_date}/"
                "extract.csv"
            )
            aws_utils.download_file(
                self.s3_bucket,
                input_csv_s3_key,
                temp_dir,
            )
            self.geolocator(
                os.path.join(temp_dir, "extract.csv"),
                os.path.join(temp_dir, "coordinates.csv"),
                self.geoloc_csv_header,
            )
            output_csv_s3_key = (
                f"{self.geoloc_key_prefix}/{self.harvest_date}/"
                "coordinates.csv"
            )
            logging.info(
                f"uploading data to {self.s3_bucket}/{output_csv_s3_key}"
            )
            client = boto3.client("s3")
            client.upload_file(
                os.path.join(temp_dir, "coordinates.csv"),
                self.s3_bucket,
                output_csv_s3_key,
            )

            logging.info("geolocation finished")
