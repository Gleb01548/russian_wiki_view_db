import datetime as dt

import pendulum
from airflow import DAG


default_args = {
    "wait_for_downstream": True,
    "retries": 10,
    "retry_delay": dt.timedelta(seconds=10),
    "execution_timeout": dt.timedelta(minutes=60),
}


dag = DAG(
    dag_id="wikiviews_processing_data_create_meassage",
    tags=[
        "test",
        "wikipedia_views",
        "analysis",
        "make_messages",
    ],
    default_args=default_args,
    start_date=pendulum.datetime(2020, 1, 1).add(days=1),
    end_date=pendulum.now("UTC").add(hours=-1),
    schedule_interval="40 * * * *",
)


