import os
from urllib import request
import datetime as dt

import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from steam.pars_steam import ParsSteam
from top_steam.constants_steam import load_url, path_save_json, path_save_script

file_name_script_save = "load_data.sql"
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
    template_searchpath=path_save_script,
)


load_data = ParsSteam(
    task_id="load_data",
    url=load_url,
    ds="{{ ds }}",
    path_save_script=os.path.join(path_save_script, file_name_script_save),
    path_save_json=path_save_json,
    dag=dag,
)

load_to_postgres = PostgresOperator(
    task_id="load_to_postgres",
    postgres_conn_id="wiki_views_postgres",
    sql=file_name_script_save,
    dag=dag,
)


load_data >> load_to_postgres
