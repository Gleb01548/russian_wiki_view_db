import os
import datetime as dt

import pendulum
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task_group

from wikiviews.make_scripts_load import MakeScriptsLoad

from constants import (
    PATH_FOR_WIKIPAGEVIEWS_GZ,
    PATH_WORK_FILES,
    DOMAIN_CODE_TIME_CORRECT,
)

path_script_load_data = os.path.join(PATH_WORK_FILES, "load_script")

default_args = {
    "wait_for_downstream": True,
    "retries": 5,
    "retry_delay": dt.timedelta(minutes=3),
    "execution_timeout": dt.timedelta(minutes=60),
    "depends_on_past": True,
    "start_date": pendulum.datetime(2020, 1, 1).add(days=-1),
    "end_date": pendulum.datetime(2021, 1, 1),
}


dag = DAG(
    dag_id="unzip_and_make_script_for_load_data",
    tags=["test", "wikipedia_views", "unzip", "load_to_postgres"],
    default_args=default_args,
    schedule_interval="@hourly",
    template_searchpath=PATH_WORK_FILES,
)

find_file = FileSensor(
    task_id="find_file_gz",
    filepath=os.path.join(
        PATH_FOR_WIKIPAGEVIEWS_GZ,
        "pageviews-{{ data_interval_start.format('YYYYMMDD-HH') }}0000.gz",
    ),
    dag=dag,
)

extract_gz = BashOperator(
    task_id="extract_gz",
    bash_command=(
        f"gunzip -fk"
        f""" {os.path.join(PATH_FOR_WIKIPAGEVIEWS_GZ,
        "pageviews-{{ data_interval_start.format('YYYYMMDD-HH') }}0000.gz")}"""
        f" && mv"
        f""" {os.path.join(PATH_FOR_WIKIPAGEVIEWS_GZ,
        "pageviews-{{ data_interval_start.format('YYYYMMDD-HH') }}0000")}"""
        f" {os.path.join(PATH_WORK_FILES, 'wikipediaviews')}"
    ),
    dag=dag,
)

make_scripts_load = MakeScriptsLoad(
    task_id="make_scripts_load",
    domain_code_time_correct=DOMAIN_CODE_TIME_CORRECT,
    file_load_path=os.path.join(PATH_WORK_FILES, "wikipediaviews"),
    path_script_load_data=path_script_load_data,
    dag=dag,
)

groups = []
for domain_code in DOMAIN_CODE_TIME_CORRECT.keys():

    @task_group(dag=dag, group_id=f"load_to_postgres_trigger_clickhouse_{domain_code}")
    def load_to_postgres_trigger_clickhouse():
        load_to_postgres = PostgresOperator(
            task_id="load_to_postgres",
            postgres_conn_id="wiki_views_postgres_resource",
            sql=f"load_script_{domain_code}.sql",
            dag=dag,
        )

        not_end_day = EmptyOperator(task_id=f"not_end_day_{domain_code}", dag=dag)
        not_end_day_2 = EmptyOperator(task_id=f"not_end_day2_{domain_code}", dag=dag)

        time_check = BranchDateTimeOperator(
            task_id="time_check",
            use_task_logical_date=True,
            follow_task_ids_if_true=[not_end_day.task_id],
            follow_task_ids_if_false=[not_end_day_2.task_id],
            target_upper=pendulum.time(23, 0, 0).add(
                hours=-DOMAIN_CODE_TIME_CORRECT[domain_code]
            ),
            target_lower=pendulum.time(23, 0, 0).add(
                hours=-DOMAIN_CODE_TIME_CORRECT[domain_code]
            ),
            dag=dag,
        )

        load_to_postgres >> time_check >> [not_end_day, not_end_day_2]

    groups.append(load_to_postgres_trigger_clickhouse())


find_file >> extract_gz >> make_scripts_load >> [task for task in groups]
