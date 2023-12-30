import os
from urllib import request
import datetime as dt

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from load_steam.pars_steam import ParsSteam
from top_steam.constants_steam import load_url, path_save_json, path_save_script


default_args = {
    "depends_on_past": True,
    "retries": 5,
    "schedule_interval": "@daily",
    "retry_delay": dt.timedelta(minutes=3),
    "execution_timeout": dt.timedelta(minutes=60),
    "wait_for_downstream": True,
}

dag = DAG(
    dag_id="top_steam",
    start_date=pendulum.now("UTC").add(days=-1),
    end_date=pendulum.now("UTC"),
    tags=["top_steam"],
    default_args=default_args,
)

load_data = ParsSteam(
    task_id="load_data",
    url=load_url,
    ds="{{ ds }}",
    path_save_script=path_save_script,
    path_save_json=path_save_json,
    dag=dag,
)
