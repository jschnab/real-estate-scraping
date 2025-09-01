import os
import sys
import time

from configparser import ConfigParser
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import boto3

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, os.path.join(str(Path.home()), "real-estate-scraping"))

import geoloc
import zizi.browse
import zizi.parse_soup

from aws_utils import download_file
from db_utils import copy_from, execute_sql, table_exists
from sql_commands import CREATE_TABLE_RENTALS_SQL
from selenium_browser import Browser

BASE_URL = "https://www.zillow.com"
CONFIG_FILE = "zillow.conf"


def browse():
    crawler = Browser(
        base_url=BASE_URL,
        stop_test=zizi.browse.is_last_page,
        get_browsable=zizi.browse.wrapper_next_page,
        get_parsable=zizi.browse.get_listings,
        get_page_id=zizi.browse.get_listing_id,
        wait_page_load=30,
        config_file=CONFIG_FILE,
        check_can_fetch=False,
    )
    crawler.browse(zizi.browse.BEGIN_RENT_LISTINGS)
    crawler.close()


def wait_queue_empty():
    client = boto3.client("cloudwatch")
    while True:
        response = client.describe_alarms(
            AlarmNames=["zillow-queue-empty"]
        )
        if response.get("MetricAlarms")[0].get("StateValue") == "ALARM":
            break
        time.sleep(600)


def extract(**context):
    crawler = Browser(
        base_url=BASE_URL,
        soup_parser=zizi.parse_soup.parse_webpage,
        harvest_date=context["ds_nodash"],
        wait_page_load=30,
        config_file=CONFIG_FILE,
        check_can_fetch=False,
    )
    crawler.extract()
    crawler.close()


def add_geolocation(**context):
    crawler = Browser(
        base_url=BASE_URL,
        harvest_date=context["ds_nodash"],
        config_file=CONFIG_FILE,
        geolocator=geoloc.add_coordinates,
        check_can_fetch=False,
    )
    crawler.geolocalize()
    crawler.close()


def load(**context):
    config_file = os.path.join(
        str(Path.home()), ".browsing",
        CONFIG_FILE,
    )
    config = ConfigParser()
    config.read(config_file)
    date_obj = datetime.strptime(context["ds_nodash"], "%Y%m%d")
    date_str = date_obj.strftime("%Y/%m/%d")
    csv_s3_key = f"coordinates/zillow/{date_str}/coordinates.csv"
    with TemporaryDirectory() as temp_dir:
        download_file(
            config["s3"]["bucket"],
            csv_s3_key,
            temp_dir,
        )
        if not table_exists("rentals"):
            execute_sql(CREATE_TABLE_RENTALS_SQL)
        local_csv_path = os.path.join(temp_dir, "coordinates.csv")
        copy_from(local_csv_path, "rentals")


default_args = {
    "depends_on_past": False,
    "start_date": days_ago(1),
}

dag = DAG(
    dag_id="zillow_scraping",
    default_args=default_args,
    schedule_interval=None,
)

browse_task = PythonOperator(
    task_id="browse",
    python_callable=browse,
    dag=dag,
)

wait_task = PythonOperator(
    task_id="wait",
    python_callable=wait_queue_empty,
    dag=dag,
)

extract_task = PythonOperator(
    task_id="extract",
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

geoloc_task = PythonOperator(
    task_id="geolocation",
    python_callable=add_geolocation,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load",
    python_callable=load,
    provide_context=True,
    dag=dag,
)

browse_task >> wait_task >> extract_task >> geoloc_task >> load_task
