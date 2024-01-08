import os
import requests
import datetime as dt
from urllib import request

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.python import PythonSensor
from airflow.decorators import task_group

from wikiviews.make_scripts_load import MakeScriptsLoad
from wikiviews.load_postgres import LoadPostgres
from wikiviews.postgresql_to_clickhouse import PostgresqlToClickhouse
from wikiviews.period_views_sum import PeriodViewsSum
from wikiviews.constants import (
    # пути
    PATH_TEMP_FILES,
    PATH_DOMAIN_DATA,
    # для подключения
    PASTGRES_CONN_ID,
    CLICKHOUSE_CONN_ID,
    # настройки
    DOMAIN_CONFIG,
)

"""
Даг качает данные с википедии, готовит скрипты, 
загружает данные в постгрес, подсчитывает просмотры по часам,
агрегирует данные по дню и загружает в кликхаус
"""


path_dz_file = os.path.join(PATH_TEMP_FILES, "pageviews.gz")
path_save_script = os.path.join(PATH_TEMP_FILES, "script_load_postgres")


default_args = {
    "wait_for_downstream": True,
    "retries": 10,
    "retry_delay": dt.timedelta(seconds=10),
    "execution_timeout": dt.timedelta(minutes=60),
}

dag = DAG(
    dag_id="load_data_wikiviews",
    tags=["wikipedia_views", "load_to_postgres", "views_in_day", "load_to_clikchouse"],
    default_args=default_args,
    start_date=pendulum.datetime(2020, 1, 1).add(days=1),
    end_date=pendulum.now("UTC").add(hours=-1),
    schedule_interval="@hourly",
)


def _сheck_data(**context):
    year, month, day, hour, *_ = context["data_interval_end"].timetuple()
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


сheck_data = PythonSensor(
    task_id="сheck_data",
    python_callable=_сheck_data,
    timeout=60 * 60 * 24,  # это 24 часа
    mode="reschedule",
    dag=dag,
)


def _get_data(**context):
    year, month, day, hour, *_ = context["data_interval_end"].timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, path_dz_file)


get_data = PythonOperator(
    task_id="load_data",
    python_callable=_get_data,
    dag=dag,
)


make_scripts_load = MakeScriptsLoad(
    task_id="make_scripts_load",
    domain_config=DOMAIN_CONFIG,
    path_dz_file=path_dz_file,
    path_save_script=path_save_script,
    dag=dag,
)


groups_load_to_postgres = []
for domain_code, config in DOMAIN_CONFIG.items():

    @task_group(dag=dag, group_id=f"load_to_postgres_trigger_clickhouse_{domain_code}")
    def load_to_postgres_trigger_clickhouse():
        load_to_postgres = LoadPostgres(
            task_id="load_to_postgres",
            path_script_load_data=f"{path_save_script}_{domain_code}.sql",
            postgres_conn_id=PASTGRES_CONN_ID,
            dag=dag,
        )

        not_end_day = EmptyOperator(task_id="not_end_day", dag=dag)
        postgres_to_clickhouse = PostgresqlToClickhouse(
            task_id="postgres_clickhouse",
            config=config,
            clickhouse_conn_id=CLICKHOUSE_CONN_ID,
            dag=dag,
        )

        time_check = BranchDateTimeOperator(
            task_id="time_check",
            use_task_logical_date=True,
            follow_task_ids_if_true=[postgres_to_clickhouse.task_id],
            follow_task_ids_if_false=[not_end_day.task_id],
            target_upper=pendulum.time(23, 0, 0).add(hours=-config["time_correction"]),
            target_lower=pendulum.time(23, 0, 0).add(hours=-config["time_correction"]),
            dag=dag,
        )
        clean_table = PostgresOperator(
            task_id="clean_table",
            postgres_conn_id=PASTGRES_CONN_ID,
            sql=(
                f"DELETE FROM resource.{domain_code} "
                "WHERE datetime::date ="
                "'{{ data_interval_start.add(hours=%(time_correction)s).format('YYYY-MM-DD') }}';"
                % {"time_correction": config["time_correction"]},
            ),
            dag=dag,
        )

        hours_views_sum = PeriodViewsSum(
            task_id="hours_views_sum",
            config=config,
            date_period_type="day",
            conn_id=PASTGRES_CONN_ID,
            path_save=PATH_DOMAIN_DATA,
        )

        load_to_postgres >> time_check >> [hours_views_sum, not_end_day]
        hours_views_sum >> postgres_to_clickhouse >> clean_table

    groups_load_to_postgres.append(load_to_postgres_trigger_clickhouse())


сheck_data >> get_data >> make_scripts_load >> groups_load_to_postgres
