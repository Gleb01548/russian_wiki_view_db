import os
import datetime as dt

import pendulum
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.operators.empty import EmptyOperator

from wikiviews.unzip_load_postgres import UnzipLoadPostgres
from wikiviews.postgresql_to_clickhouse import PostgresqlToClickhouse

from constants import (
    PATH_FOR_WIKIPAGEVIEWS_GZ,
    PATH_WORK_FILES,
    DOMAIN_CONFIG,
    PASTGRES_CONN_ID,
    CLICKHOUSE_CONN_ID,
)


default_args = {
    "wait_for_downstream": True,
    "retries": 5,
    "retry_delay": dt.timedelta(minutes=3),
    "execution_timeout": dt.timedelta(minutes=60),
    "depends_on_past": True,
    "end_date": pendulum.datetime(2021, 1, 1),
}

domain_code = "ru"
dag_ru = DAG(
    dag_id=f"{domain_code}_unzip_and_load_data_postgresql",
    tags=[
        "test",
        "wikipedia_views",
        "unzip",
        "load_to_postgres",
        domain_code,
    ],
    default_args=default_args,
    start_date=pendulum.datetime(2020, 1, 1).add(
        hours=DOMAIN_CONFIG[domain_code]["time_correction"]
    ),
    schedule_interval="@hourly",
    template_searchpath=PATH_WORK_FILES,
)
DOMAIN_CONFIG[domain_code]["dag"] = dag_ru


domain_code = "en"
dag_en = DAG(
    dag_id=f"{DOMAIN_CONFIG[domain_code]['domain_code']}_unzip_and_load_data_postgresql",
    tags=[
        "test",
        "wikipedia_views",
        "unzip",
        "load_to_postgres",
        domain_code,
    ],
    default_args=default_args,
    start_date=pendulum.datetime(2020, 1, 1).add(
        hours=DOMAIN_CONFIG[domain_code]["time_correction"]
    ),
    schedule_interval="@hourly",
    template_searchpath=PATH_WORK_FILES,
)
DOMAIN_CONFIG[domain_code]["dag"] = dag_en

path_script_load_data = os.path.join(PATH_WORK_FILES, "load_script")
for domain_code, config in DOMAIN_CONFIG.items():
    find_file = FileSensor(
        task_id="find_file_gz",
        filepath=os.path.join(
            PATH_FOR_WIKIPAGEVIEWS_GZ,
            "pageviews-{{ data_interval_end.format('YYYYMMDD-HH') }}0000.gz",
        ),
        dag=config["dag"],
    )

    make_scripts_load = UnzipLoadPostgres(
        task_id="make_scripts_load",
        config=config,
        path_dz_file=os.path.join(
            PATH_FOR_WIKIPAGEVIEWS_GZ,
            "pageviews-{{ data_interval_end.format('YYYYMMDD-HH') }}0000.gz",
        ),
        postgres_conn_id=PASTGRES_CONN_ID,
        path_script_load_data=path_script_load_data,
        dag=config["dag"],
    )

    time_check = BranchDateTimeOperator(
        task_id="time_check",
        use_task_logical_date=True,
        follow_task_ids_if_true=["postgres_to_clickhouse"],
        follow_task_ids_if_false=["not_end_day"],
        target_upper=pendulum.time(23, 0, 0).add(hours=-config["time_correction"]),
        target_lower=pendulum.time(23, 0, 0).add(hours=-config["time_correction"]),
        dag=config["dag"],
    )
    not_end_day = EmptyOperator(task_id="not_end_day", dag=config["dag"])

    postgres_to_clickhouse = PostgresqlToClickhouse(
        task_id="postgres_to_clickhouse",
        config=config,
        postgres_conn_id=PASTGRES_CONN_ID,
        clickhouse_conn_id=CLICKHOUSE_CONN_ID,
        dag=config["dag"],
    )

    (
        find_file
        >> make_scripts_load
        >> time_check
        >> [postgres_to_clickhouse, not_end_day]
    )


# make_scripts_load = MakeScriptsLoad(
#     task_id="make_scripts_load",
#     domain_code_time_correct=DOMAIN_CODE_TIME_CORRECT,
#     file_load_path=os.path.join(PATH_WORK_FILES, "wikipediaviews"),
#     path_script_load_data=path_script_load_data,
#     dag=dag,
# )

# groups = []
# for domain_code in DOMAIN_CODE_TIME_CORRECT.keys():

#     @task_group(dag=dag, group_id=f"load_to_postgres_trigger_clickhouse_{domain_code}")
#     def load_to_postgres_trigger_clickhouse():
#         load_to_postgres = PostgresOperator(
#             task_id="load_to_postgres",
#             postgres_conn_id="wiki_views_postgres_resource",
#             sql=f"load_script_{domain_code}.sql",
#             dag=dag,
#         )

#         not_end_day = EmptyOperator(task_id=f"not_end_day_{domain_code}", dag=dag)
#         not_end_day_2 = EmptyOperator(task_id=f"not_end_day2_{domain_code}", dag=dag)

#         time_check = BranchDateTimeOperator(
#             task_id="time_check",
#             use_task_logical_date=True,
#             follow_task_ids_if_true=[not_end_day.task_id],
#             follow_task_ids_if_false=[not_end_day_2.task_id],
#             target_upper=pendulum.time(23, 0, 0).add(
#                 hours=-DOMAIN_CODE_TIME_CORRECT[domain_code]
#             ),
#             target_lower=pendulum.time(23, 0, 0).add(
#                 hours=-DOMAIN_CODE_TIME_CORRECT[domain_code]
#             ),
#             dag=dag,
#         )

#         load_to_postgres >> time_check >> [not_end_day, not_end_day_2]

#     groups.append(load_to_postgres_trigger_clickhouse())


# find_file >> extract_gz >> make_scripts_load >> [task for task in groups]
