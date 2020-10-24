import bz2
import csv
import glob
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
from selenium import webdriver

import aws_utils

from utils import cut_url, Explored, timeout

CONFIG_DIR = os.path.join(str(Path.home()), ".browsing")
HARVEST_PAUSE_BACKOFF = 0.3
HARVEST_PAUSE_MAX = 60 * 30  # 30 minutes


class Browser:
    def __init__(
        self,
        base_url,
        stop_test=None,
        get_browsable=None,
        get_parsable=None,
        get_page_id=None,
        max_retries=5,
        browse_delay=0,
        check_can_fetch=True,
        wait_page_load=20,
        html_parser="html.parser",
        soup_parser=None,
        geolocator=None,
        config_file="browser.conf",
        override_user_agents=True,
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
        :param int max_retries: maximum number of retries on request failure
        :param list[int] retry_on: HTTP status codes allowing request retry
        :param float browse_delay: time in seconds to wait between page
                                   downloads
        :param bool check_can_fetch: check if robots.txt allows scraping
                                     before downloading a page
        :param float wait_page_load: time in seconds to wait for a page to
                                     load before downloading contents
        :param str html_parser: parser to use with BeautifulSoup, e.g.
                                'html.parser', 'lxml', etc
        :param callable soup_parser: function to use to parse the HTML tags
                                     soup into a dictionary
        :param callable geolocator: function which adds latitude and longitude
                                    to a home listing
        :param str config_file: name of the configuration file
        :param bool override_user_agents: if True, overrides the native user
                                          agent of the Selenium webdriver
        :param str harvest_date: date of harvest, format YYYYMMDD
        """
        self.base_url = base_url
        self.stop_test = stop_test
        self.get_browsable = get_browsable
        self.get_parsable = get_parsable
        self.get_page_id = get_page_id
        self.max_retries = max_retries
        self.check_can_fetch = check_can_fetch
        self.wait_page_load = wait_page_load
        self.soup_parser = soup_parser
        self.to_browse = deque()
        self.sqs_client = boto3.client("sqs")
        self.s3_client = boto3.client("s3")
        self.explored = Explored()
        self.harvest_pauses = 0
        self.override_user_agents = override_user_agents
        self.harvest_date = self.set_harvest_date(harvest_date)
        if not html_parser:
            self.html_parser = partial(BeautifulSoup, features="html.parser")
        else:
            self.html_parser = partial(BeautifulSoup, features=html_parser)
        self.geolocator = geolocator

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
        user_agents_path = os.path.join(CONFIG_DIR, "user_agents")
        if Path(user_agents_path).exists():
            logging.info(f"reading user agents '{user_agents_path}'")
            self.user_agents = self.get_user_agents(user_agents_path)
        else:
            self.user_agents = []
        self.config_path = os.path.join(CONFIG_DIR, config_file)
        if Path(self.config_path).exists():
            logging.info(f"reading config '{self.config_path}'")
            self.configure(self.config_path)
        else:
            logging.warning(f"config file not found: '{self.config_path}'")

    def get_user_agents(self, user_agents_path):
        """
        Get the list of user agents from a JSON file.

        :param str user_agents_path: path of the file storing user agents
        :returns (list): list of user agents
        """
        with open(user_agents_path) as f:
            return json.load(f)

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

        # configure webdriver
        # user_data_dir = os.path.join(CONFIG_DIR, "user_data")
        driver_path = conf["selenium"]["driver_path"]
        options = webdriver.FirefoxOptions()
        options.add_argument("--headless")
        options.add_argument("--window-size=1420,1080")
        options.add_argument("--disable-extensions")
        # options.add_argument("--incognito")
        options.add_argument("--disable-plugins-discovery")
        # options.add_argument(f"--user-data-dir={user_data_dir}")
        # options.add_experimental_option(
        #    "excludeSwitches",
        #    ["enable-automation"]
        # )

        profile = webdriver.FirefoxProfile()
        if self.override_user_agents and self.user_agents:
            self.user_agent = random.choice(self.user_agents)
            logging.info(f"using User-Agent: {self.user_agent}")
            profile.set_preference(
                "general.useragent.override",
                self.user_agent
            )

        self.webdriver = webdriver.Firefox(
            executable_path=driver_path,
            options=options,
            firefox_profile=profile,
            log_path=os.path.join(CONFIG_DIR, "geckodriver.log"),
        )

        # use native webdriver user agent if we did not override
        if not (self.override_user_agents and self.user_agents):
            self.user_agent = self.webdriver.execute_script(
               "return navigator.userAgent"
            )
            logging.info(f"using User-Agent: {self.user_agent}")

        logging.basicConfig(
            format="%(asctime)s %(levelname)s %(message)s",
            level=logging.INFO,
            filename=os.path.join(CONFIG_DIR, "browser.log"),
            filemode="a")

    def close(self):
        """
        Close the webdriver.
        This is important to avoid accumulation of webdriver processes.
        """
        self.webdriver.close()

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

    @timeout(60)
    def get_page_contents(self, url):
        self.webdriver.get(url)
        time.sleep(random.gauss(self.wait_page_load, self.wait_page_load / 6))
        return self.webdriver.page_source

    def download_page(self, url):
        """
        Download a web page.
        """
        if not url.startswith(self.base_url):
            url = urljoin(self.base_url, url)
        for i in range(self.max_retries):
            try:
                return self.get_page_contents(url)
            except Exception as e:
                logging.error(
                    f"{e}: retry {i+1}/{self.max_retries} downloading "
                    f"{cut_url(url)} failed"
                )
                continue
        logging.error(f"too many retries downloading {cut_url(url)}")
        return

    def browse(self, initial=None):
        """
        Crawl a website in a breadth-first search fashion.

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
            if self.check_can_fetch:
                if not self.can_fetch(self.user_agent, current):
                    logging.info(f"forbidden: {cut_url(current)}")
                    continue

            logging.info(f"downloading {cut_url(current)}")
            content = self.download_page(current)
            time.sleep(random.gauss(self.browse_delay, self.browse_delay / 6))

            # if download failed, push URL back to queue
            if content is None:
                logging.info("pushing URL back into queue")
                self.to_browse.appendleft(current)
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
                self.to_browse.appendleft(child)

    def harvest(self):
        """
        Download the web pages stored in an SQS queue.
        """
        logging.info("start harvesting")
        while True:
            handle, current = self.pop_queue()
            if not current:
                logging.info("no message received, pausing")
                self.pause_harvest()
                continue
            self.harvest_pauses = 0
            if self.check_can_fetch:
                if not self.can_fetch(self.user_agent, current):
                    logging.info(f"forbidden: {cut_url(current)}")
                    self.delete_message(handle)
                    continue

            logging.info(f"downloading {cut_url(current)}")
            content = self.download_page(current)
            time.sleep(random.gauss(self.browse_delay, self.browse_delay / 6))

            # if download failed, push URL back to queue
            if content is None:
                logging.info("pushing URL back to queue")
                self.delete_message(handle)
                self.push_queue(current)
                continue

            file_prefix = self.get_page_id(current)
            logging.info(f"archiving {file_prefix}")
            self.store_harvest(file_prefix, content.encode("utf8"))
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
            logging.info(
                f"uploading data to s3://{self.s3_bucket}/{csv_s3_key}"
            )
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
