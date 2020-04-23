import os
import time

from configparser import ConfigParser
from datetime import datetime
from pathlib import Path

import boto3

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import aws_utils
import geoloc

from nytimes.browse import *
from nytimes.parse_soup import *
from tor_sqs_browser import Browser

HOME = str(Path.home())


def browse():
    crawler = Browser(
        base_url="https://www.nytimes.com",
        stop_test=is_last_page,
        get_browsable=wrapper_next_page,
        get_parsable=get_listings,
        get_page_id=get_listing_id,
    )
    crawler.browse(BEGIN_RENT_LISTINGS)


def wait_queue_empty():
    client = boto3.client("cloudwatch")
    while True:
        response = client.describe_alarms(AlarmNames=["queue-empty"])
        if response.get("MetricAlarms")[0].get("StateValue") == "ALARM":
            break
        time.sleep(600)


def extract(**context):
    crawler = Browser(
        base_url="https://www.nytimes.com",
        stop_test=is_last_page,
        get_browsable=wrapper_next_page,
        get_parsable=get_listings,
        get_page_id=get_listing_id,
        soup_parser=parse_webpage,
        harvest_date=context["ds_nodash"]
    )
    crawler.extract()


def add_coordinates(**context):
    config = ConfigParser()
    config.read(os.path.join(HOME, '.browsing', 'browser.conf'))
    date = datetime.strptime(context["ds_nodash"], "%Y%m%d")

    aws_utils.download_file(
        bucket=config["s3"]["bucket"],
        key="real-estate/nytimes/extract/{date.strftime('%Y/%m/%d')}/extract.csv",
        destination=os.path.join(HOME, "real-estate-scraping"),
    )

    geoloc.add_coordinates(
        input_csv=os.path.join(HOME, 'real-estate-scraping', 'extract.csv'),
        output_csv=os.path.join(HOME, 'real-estate-scraping', 'coordinates.csv'),
        columns=config['geolocation']['csv_header'].split(','),
        geocode=geoloc.query_bing_maps,
        api_key=config['geolocation']['bing_maps_key'],
    )

    aws_utils.upload_file(
        source=os.path.join(HOME, "real-estate-scraping", "coordinates.csv"),
        bucket=config["s3"]["bucket"],
        key="real-estate/nytimes/coordinates/{date.strftime('%Y/%m/%d')}/coordinates.csv",
    )


def add_annotation(**context):
    config = ConfigParser()
    config.read(os.path.join(HOME, '.browsing', 'browser.conf'))
    date = datetime.strptime(context["ds_nodash"], "%Y%m%d")

    aws_utils.download_file(
        bucket=config["s3"]["bucket"],
        key="real-estate/nytimes/coordinates/{date.strftime('%Y/%m/%d')}/coordinates.csv",
        destination=os.path.join(HOME, "real-estate-scraping"),
    )

    add_yelp_annotation(
        input_csv=os.path.join(HOME, "real-estate-scraping", "coordinates.csv"),
        output_csv=os.path.join(HOME, "real-estate-scraping", "annotated.csv"),
        columns=config['yelp']['csv_header'].split(','),
    )

    aws_utils.upload_file(
        source=os.path.join(HOME, "real-estate-scraping", "annotated.csv"),
        bucket=config["s3"]["bucket"],
        key="real-estate/nytimes/annotation/{date.strftime('%Y/%m/%d')}/annotated.csv",
    )


default_args = {
    "depends_on_past": False,
    "start_date": days_ago(1),
}

dag = DAG(
    dag_id="real_estate_scraping",
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
    python_callable=add_coordinates,
    provide_context=True,
    dag=dag,
)

annotation_task = PythonOperator(
    task_id="annotation",
    python_callable=add_annotation,
    provide_context=True,
    dag=dag,
)

browse_task >> wait_task >> extract_task >> geoloc_task >> annotation_task
