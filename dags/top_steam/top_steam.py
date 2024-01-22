import os
from urllib import request
import datetime as dt

import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator

from steam.pars_steam import ParsSteam
from top_steam.constants_steam import load_url, path_save_json, path_save_script

file_name_script_save = "load_data.sql"
default_args = {
    "depends_on_past": True,
    "retries": 5,
    "retry_delay": dt.timedelta(minutes=3),
    "execution_timeout": dt.timedelta(minutes=60),
    "wait_for_downstream": True,
}

dag = DAG(
    dag_id="top_steam",
    start_date=pendulum.datetime(2024, 1, 1).add(months=-1),
    end_date=pendulum.now("UTC"),
    tags=["steam", "top_steam"],
    schedule_interval="20 1 * * *",
    default_args=default_args,
    max_active_runs=1,
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

send_message_telegram_task = TelegramOperator(
    task_id="send_message_telegram",
    token=os.environ.get("TELEGRAM_NOTIFICATION"),
    chat_id=os.environ.get("CHANNEL_AIRFLOW"),
    text=f"Wiki views problem with DAG:{dag.dag_id}",
    dag=dag,
    trigger_rule="one_failed",
)

load_data >> load_to_postgres >> send_message_telegram_task
