import os
import datetime as dt

import pendulum
from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.decorators import task_group

from steam.analysis_data import Analysis
from steam.create_message import CreateMessage
from steam.send_message_steam import SendMessages
from top_steam.constants_steam import (
    path_save_analysis,
    path_save_messages,
    MESSAGE_CONFIG,
    columns_for_table_day,
    columns_increment_for_table_day,
    columns_for_table_not_day,
    columns_increment_for_table_not_day,
    columns_increment_for_table_csv_not_day,
)

default_args = {
    "depends_on_past": True,
    "retries": 5,
    "schedule_interval": "@daily",
    "retry_delay": dt.timedelta(minutes=3),
    "execution_timeout": dt.timedelta(minutes=60),
    "wait_for_downstream": True,
}

dag = DAG(
    dag_id="analysis_steam",
    start_date=pendulum.datetime(2024, 1, 1),
    end_date=pendulum.now("UTC"),
    tags=["steam", "analysis_steam"],
    max_active_runs=1,
    default_args=default_args,
)


def _find_new_date(**context):
    data = context["data_interval_start"]
    print(data)
    ph = PostgresHook(postgres_conn_id="wiki_views_postgres")
    result = ph.get_records(
        f"SELECT COUNT(*) FROM steam.steam_data WHERE date = '{data}'"
    )
    print(result)
    return result[0][0] > 1


sensor = PythonSensor(
    task_id="sensor",
    python_callable=_find_new_date,
    timeout=60 * 60 * 24,  # это 24 часа
    mode="reschedule",
)

analysis_day = Analysis(
    task_id="analysis_day",
    postgres_conn_id="wiki_views_postgres",
    date_period_type="day",
    path_save=path_save_analysis,
    dag=dag,
)

analysis_week = Analysis(
    task_id="analysis_week",
    postgres_conn_id="wiki_views_postgres",
    date_period_type="week",
    path_save=path_save_analysis,
    dag=dag,
)

analysis_month = Analysis(
    task_id="analysis_month",
    postgres_conn_id="wiki_views_postgres",
    date_period_type="month",
    path_save=path_save_analysis,
    dag=dag,
)

analysis_year = Analysis(
    task_id="analysis_year",
    postgres_conn_id="wiki_views_postgres",
    date_period_type="year",
    path_save=path_save_analysis,
    dag=dag,
)


def _check_end_week_month_year(**context):
    list_branches = [analysis_day.task_id]

    data_interval_start = context["data_interval_start"]
    if data_interval_start.day_of_week == 0:
        list_branches.append(analysis_week.task_id)

    if data_interval_start.day == data_interval_start.days_in_month:
        list_branches.append(analysis_month.task_id)

    if data_interval_start.year != data_interval_start.add(days=1).year:
        list_branches.append(analysis_year.task_id)

    return list_branches


end_analysis = EmptyOperator(
    task_id="end_analysis", dag=dag, trigger_rule="none_failed"
)

check_end_week_month_year = BranchPythonOperator(
    task_id="check_end_week_month_year",
    python_callable=_check_end_week_month_year,
    dag=dag,
)


domain_groups = []
for domain_code, config in MESSAGE_CONFIG.items():

    @task_group(dag=dag, group_id=f"create_message_{domain_code}")
    def create_message_domain():
        create_message_day = CreateMessage(
            task_id="create_message_day",
            config=config,
            path_save=path_save_messages,
            path_for_json=path_save_analysis,
            date_period_type="day",
            domain_code=domain_code,
            columns_for_table=columns_for_table_day,
            columns_increment_for_table=columns_increment_for_table_day,
            columns_increment_for_table_csv=columns_increment_for_table_day,
            dag=dag,
        )

        create_message_week = CreateMessage(
            task_id="create_message_week",
            config=config,
            path_save=path_save_messages,
            path_for_json=path_save_analysis,
            date_period_type="week",
            domain_code=domain_code,
            columns_for_table=columns_for_table_not_day,
            columns_increment_for_table=columns_increment_for_table_not_day,
            columns_increment_for_table_csv=columns_increment_for_table_csv_not_day,
            dag=dag,
        )

        create_message_month = CreateMessage(
            task_id="create_message_month",
            config=config,
            path_save=path_save_messages,
            path_for_json=path_save_analysis,
            date_period_type="month",
            domain_code=domain_code,
            columns_for_table=columns_for_table_not_day,
            columns_increment_for_table=columns_increment_for_table_not_day,
            columns_increment_for_table_csv=columns_increment_for_table_csv_not_day,
            dag=dag,
        )

        create_message_year = CreateMessage(
            task_id="create_message_year",
            config=config,
            path_save=path_save_messages,
            path_for_json=path_save_analysis,
            date_period_type="year",
            domain_code=domain_code,
            columns_for_table=columns_for_table_not_day,
            columns_increment_for_table=columns_increment_for_table_not_day,
            columns_increment_for_table_csv=columns_increment_for_table_csv_not_day,
            dag=dag,
        )

        def _check_end_week_month_year(**context):
            list_branches = [create_message_day.task_id]

            data_interval_start = context["data_interval_start"]
            if data_interval_start.day_of_week == 0:
                list_branches.append(create_message_week.task_id)

            if data_interval_start.day == data_interval_start.days_in_month:
                list_branches.append(create_message_month.task_id)

            if data_interval_start.year != data_interval_start.add(days=1).year:
                list_branches.append(create_message_year.task_id)

            return list_branches

        check_end_week_month_year = BranchPythonOperator(
            task_id="check_end_week_month_year",
            python_callable=_check_end_week_month_year,
            dag=dag,
        )

        send_day = SendMessages(
            task_id="send_message_day",
            domain_code=domain_code,
            date_period_type="day",
            path_save=path_save_messages,
            telegram_token_bot=os.environ.get(config["bot_token"]),
            chat_id=os.environ.get(config["telegram_id"]),
        )

        send_week = SendMessages(
            task_id="send_message_week",
            domain_code=domain_code,
            date_period_type="week",
            path_save=path_save_messages,
            telegram_token_bot=os.environ.get(config["bot_token"]),
            chat_id=os.environ.get(config["telegram_id"]),
        )

        send_month = SendMessages(
            task_id="send_message_month",
            domain_code=domain_code,
            date_period_type="month",
            path_save=path_save_messages,
            telegram_token_bot=os.environ.get(config["bot_token"]),
            chat_id=os.environ.get(config["telegram_id"]),
        )

        send_year = SendMessages(
            task_id="send_message_year",
            domain_code=domain_code,
            date_period_type="year",
            path_save=path_save_messages,
            telegram_token_bot=os.environ.get(config["bot_token"]),
            chat_id=os.environ.get(config["telegram_id"]),
        )

        end_week = EmptyOperator(
            task_id="end_week", dag=dag, trigger_rule="none_failed"
        )
        end_month = EmptyOperator(
            task_id="end_month", dag=dag, trigger_rule="none_failed"
        )

        check_end_week_month_year >> [
            create_message_day,
            create_message_week,
            create_message_month,
            create_message_year,
        ]
        create_message_day >> send_day
        create_message_week >> send_week >> end_week
        create_message_month >> send_month >> end_month
        create_message_year >> send_year

        send_day >> send_week
        end_week >> send_month
        end_month >> send_year

    domain_groups.append(create_message_domain())


(
    sensor
    >> check_end_week_month_year
    >> [analysis_week, analysis_month, analysis_year, analysis_day]
    >> end_analysis
    >> domain_groups
)
