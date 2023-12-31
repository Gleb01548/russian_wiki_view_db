import os
import datetime as dt

import requests
import pendulum
from airflow import DAG
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from urllib import request
from airflow.decorators import task_group
from airflow.providers.telegram.operators.telegram import TelegramOperator

from wikiviews.make_scripts_load import MakeScriptsLoad
from wikiviews.load_postgres import LoadPostgres
from wikiviews.postgresql_to_clickhouse import PostgresqlToClickhouse
from wikiviews.aggregation_load_postgres import АggregationLoadPostgres
from wikiviews.analysis import Analysis
from wikiviews.translate import Translate
from wikiviews.create_massage import CreateMessage


from constants import (
    PATH_WORK_FILES,
    DOMAIN_CONFIG,
    PASTGRES_CONN_ID,
    CLICKHOUSE_CONN_ID,
)

path_save_script = os.path.join(
    PATH_WORK_FILES,
    "script_load_postgres",
)
path_dz_file = os.path.join(
    PATH_WORK_FILES,
    "pageviews.gz",
)
path_analysis_save = os.path.join(
    PATH_WORK_FILES,
    "analysis",
)


default_args = {
    "wait_for_downstream": True,
    "retries": 10,
    "retry_delay": dt.timedelta(seconds=10),
    "execution_timeout": dt.timedelta(minutes=60),
}

dag = DAG(
    dag_id="wikipedia_views_load_and_processing",
    tags=[
        "test",
        "wikipedia_views",
        "unzip",
        "load_to_postgres",
        "load_to_clikchouse",
        "translation",
        "make_messages",
    ],
    default_args=default_args,
    start_date=pendulum.datetime(2020, 1, 1).add(days=1),
    end_date=pendulum.now("UTC").add(hours=-1),
    schedule_interval="@hourly",
    template_searchpath=PATH_WORK_FILES,
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


сheck_data = PythonSensor(task_id="сheck_data", python_callable=_сheck_data, dag=dag)


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

        aggregation_date_day = АggregationLoadPostgres(
            task_id="aggregate_date_day",
            clickhouse_conn_id=CLICKHOUSE_CONN_ID,
            date_period_type="day",
            query_type="aggregate_date",
            config=config,
            dag=dag,
        )

        sum_views_day = АggregationLoadPostgres(
            task_id="sum_views_day",
            clickhouse_conn_id=CLICKHOUSE_CONN_ID,
            date_period_type="day",
            query_type="sum_views",
            config=config,
            dag=dag,
        )

        aggregation_date_week = АggregationLoadPostgres(
            task_id="aggregate_date_week",
            clickhouse_conn_id=CLICKHOUSE_CONN_ID,
            date_period_type="week",
            query_type="aggregate_date",
            config=config,
            dag=dag,
        )

        sum_views_week = АggregationLoadPostgres(
            task_id="sum_views_week",
            clickhouse_conn_id=CLICKHOUSE_CONN_ID,
            date_period_type="week",
            query_type="sum_views",
            config=config,
            dag=dag,
        )

        aggregation_date_month = АggregationLoadPostgres(
            task_id="aggregate_date_month",
            clickhouse_conn_id=CLICKHOUSE_CONN_ID,
            date_period_type="month",
            query_type="aggregate_date",
            config=config,
            dag=dag,
        )

        sum_views_month = АggregationLoadPostgres(
            task_id="sum_views_month",
            clickhouse_conn_id=CLICKHOUSE_CONN_ID,
            date_period_type="month",
            query_type="sum_views",
            config=config,
            dag=dag,
        )

        aggregation_date_year = АggregationLoadPostgres(
            task_id="aggregate_date_year",
            clickhouse_conn_id=CLICKHOUSE_CONN_ID,
            date_period_type="year",
            query_type="aggregate_date",
            config=config,
            dag=dag,
        )

        sum_views_year = АggregationLoadPostgres(
            task_id="sum_views_year",
            clickhouse_conn_id=CLICKHOUSE_CONN_ID,
            date_period_type="year",
            query_type="sum_views",
            config=config,
            dag=dag,
        )

        end_week = EmptyOperator(task_id="end_week", dag=dag)
        end_month = EmptyOperator(task_id="end_month", dag=dag)
        end_year = EmptyOperator(task_id="end_year", dag=dag)

        not_end_week_month_year = EmptyOperator(
            task_id="not_end_week_month_year", dag=dag
        )

        def _check_end_week_month_year(time_correction: dict, **context):
            list_branches = []

            data_interval_start = context["data_interval_start"].add(
                hours=time_correction
            )
            if data_interval_start.day_of_week == 0:
                list_branches.append(end_week.task_id)

            if data_interval_start.day == data_interval_start.days_in_month:
                list_branches.append(end_month.task_id)

            if data_interval_start.year != data_interval_start.add(days=1).year:
                list_branches.append(end_year.task_id)

            if list_branches:
                return list_branches
            else:
                return not_end_week_month_year.task_id

        check_end_week_month_year = BranchPythonOperator(
            task_id="check_end_week_month_year",
            python_callable=_check_end_week_month_year,
            op_args=[config["time_correction"]],
        )

        analysis_day = Analysis(
            task_id="analysis_day",
            config=config,
            postgres_conn_id=PASTGRES_CONN_ID,
            clickhouse_conn_id=CLICKHOUSE_CONN_ID,
            date_period_type="day",
            path_save=path_analysis_save,
            dag=dag,
        )

        analysis_week = Analysis(
            task_id="analysis_week",
            config=config,
            postgres_conn_id=PASTGRES_CONN_ID,
            clickhouse_conn_id=CLICKHOUSE_CONN_ID,
            date_period_type="week",
            path_save=path_analysis_save,
            dag=dag,
        )

        analysis_month = Analysis(
            task_id="analysis_month",
            config=config,
            postgres_conn_id=PASTGRES_CONN_ID,
            clickhouse_conn_id=CLICKHOUSE_CONN_ID,
            date_period_type="month",
            path_save=path_analysis_save,
            dag=dag,
        )

        analysis_year = Analysis(
            task_id="analysis_year",
            config=config,
            postgres_conn_id=PASTGRES_CONN_ID,
            clickhouse_conn_id=CLICKHOUSE_CONN_ID,
            date_period_type="year",
            path_save=path_analysis_save,
            dag=dag,
        )

        translate_day = Translate(
            task_id="translate_day",
            config=config,
            date_period_type="day",
            path_save=path_analysis_save,
            path_load_data=path_analysis_save,
        )

        translate_week = Translate(
            task_id="translate_week",
            config=config,
            date_period_type="week",
            path_save=path_analysis_save,
            path_load_data=path_analysis_save,
        )

        translate_month = Translate(
            task_id="translate_month",
            config=config,
            date_period_type="month",
            path_save=path_analysis_save,
            path_load_data=path_analysis_save,
        )

        translate_year = Translate(
            task_id="translate_year",
            config=config,
            date_period_type="year",
            path_save=path_analysis_save,
            path_load_data=path_analysis_save,
        )

        message_day = CreateMessage(
            task_id="message_day",
            config=config,
            global_config=DOMAIN_CONFIG,
            path_save=path_analysis_save,
            date_period_type="day",
        )

        message_week = CreateMessage(
            task_id="message_week",
            config=config,
            global_config=DOMAIN_CONFIG,
            path_save=path_analysis_save,
            date_period_type="week",
        )

        message_month = CreateMessage(
            task_id="message_month",
            config=config,
            global_config=DOMAIN_CONFIG,
            path_save=path_analysis_save,
            date_period_type="month",
        )

        message_year = CreateMessage(
            task_id="message_year",
            config=config,
            global_config=DOMAIN_CONFIG,
            path_save=path_analysis_save,
            date_period_type="year",
        )

        (load_to_postgres >> time_check >> [postgres_to_clickhouse, not_end_day])

        postgres_to_clickhouse >> [
            aggregation_date_day,
            sum_views_day,
            clean_table,
            check_end_week_month_year,
        ]
        [aggregation_date_day, sum_views_day] >> analysis_day
        check_end_week_month_year >> [
            end_week,
            end_month,
            end_year,
            not_end_week_month_year,
        ]
        end_week >> [aggregation_date_week, sum_views_week]
        end_month >> [aggregation_date_month, sum_views_month]
        end_year >> [aggregation_date_year, sum_views_year]

        [aggregation_date_week, sum_views_week] >> analysis_week
        [aggregation_date_month, sum_views_month] >> analysis_month
        [aggregation_date_year, sum_views_year] >> analysis_year

        analysis_day >> translate_day
        analysis_week >> translate_week
        analysis_month >> translate_month
        analysis_year >> translate_year

        translate_day >> message_day
        translate_week >> message_week
        translate_month >> message_month
        translate_year >> message_year

    groups_load_to_postgres.append(load_to_postgres_trigger_clickhouse())


trigger_dag = TriggerDagRunOperator(
    task_id="trigger_send_message",
    trigger_dag_id="send_message",
    execution_date="{{ data_interval_start }}",
    reset_dag_run=True,
    dag=dag,
    wait_for_completion=False,
)

not_end_day = EmptyOperator(task_id="not_end_day", dag=dag)


def _check_end_day(time_correction_list: list, **context):
    for time_correction in time_correction_list:
        data_interval_start = context["data_interval_start"].add(hours=time_correction)
        if data_interval_start.hour == 23:
            return trigger_dag.task_id

    return not_end_day.task_id


check_end_day = BranchPythonOperator(
    task_id="check_end_day",
    python_callable=_check_end_day,
    op_args=[[val["time_correction"] for val in DOMAIN_CONFIG.values()]],
    dag=dag,
    trigger_rule="none_failed",
)

send_message_telegram_task = TelegramOperator(
    task_id="send_message_telegram",
    token=os.environ.get("TELEGRAM_NOTIFICATION"),
    chat_id=os.environ.get("CHANNEL_AIRFLOW"),
    text=f"Wiki views problem with DAG:{dag.dag_id}",
    dag=dag,
    trigger_rule="one_failed",
)

(
    сheck_data
    >> get_data
    >> make_scripts_load
    >> groups_load_to_postgres
    >> check_end_day
    >> [trigger_dag, not_end_day]
    >> send_message_telegram_task
)
