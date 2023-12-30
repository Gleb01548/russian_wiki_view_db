import os
import requests
from urllib import request
import datetime as dt
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from constants import PATH_FOR_WIKIPAGEVIEWS_GZ


default_args = {
    "depends_on_past": True,
    "retries": 5,
    "retry_delay": dt.timedelta(minutes=3),
    "execution_timeout": dt.timedelta(minutes=60),
}

dag = DAG(
    dag_id="load_from_wikipageviews",
    start_date=pendulum.datetime(2019, 1, 1).add(days=-1),
    end_date=pendulum.now("UTC"),
    schedule_interval="@hourly",
    tags=["wikipageviews"],
    default_args=default_args,
)


def _сheck_data(**context):
    year, month, day, hour, *_ = context["data_interval_start"].timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    try:
        res = requests.head(url)
        return res.ok
    except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout):
        return False


сheck_data = PythonSensor(task_id="сheck_data", python_callable=_сheck_data, dag=dag)


def _get_data(**context):
    year, month, day, hour, *_ = context["data_interval_start"].timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )

    file_name_result = os.path.join(
        PATH_FOR_WIKIPAGEVIEWS_GZ,
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz",
    )
    request.urlretrieve(url, file_name_result)


get_data = PythonOperator(
    task_id="load_data",
    python_callable=_get_data,
    dag=dag,
)


сheck_data >> get_data
