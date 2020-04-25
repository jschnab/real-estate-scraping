import time

from pathlib import Path

import boto3

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

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


def add_geolocation(**context):
    crawler = Browser(
        base_url="https://www.nytimes.com",
        harvest_date=context["ds_nodash"]
    )
    crawler.geolocalize()


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
    python_callable=add_geolocation,
    provide_context=True,
    dag=dag,
)

browse_task >> wait_task >> extract_task >> geoloc_task
