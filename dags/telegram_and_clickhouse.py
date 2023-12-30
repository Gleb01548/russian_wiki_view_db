import os
import datetime as dt

import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from russian_wiki.message_in_telegram import MessegeTelegramNViews
from russian_wiki.postgresql_to_clickhouse import PostgresqlToClickhouse


default_args = {
    "wait_for_downstream": True,
    "retries": 5,
    "retry_delay": dt.timedelta(minutes=3),
    "execution_timeout": dt.timedelta(minutes=60),
    "depends_on_past": True,
}


dag = DAG(
    dag_id="telegram_and_clickhouse",
    start_date=dt.datetime(2022, 1, 1),
    end_date=pendulum.now("UTC"),
    schedule_interval=None,
    tags=["russian_wikipedia", "telegram", "clickhouse"],
    default_args=default_args,
)

query_telegram = MessegeTelegramNViews(
    task_id="query_telegram",
    postgres_conn_id="russian_wiki_postgres",
    telegram_token_bot=os.environ.get("TELEGRAM_TOKEN_BOT"),
    telegram_chat_id=os.environ.get("TLEGRAM_CHAT_ID"),
    ds="{{ ds }}",
    n_pages=100,
    dag=dag,
)

postgresql_to_clickhouse = PostgresqlToClickhouse(
    task_id="postgresql_to_clickhouse",
    postgres_conn_id="russian_wiki_postgres",
    clickhouse_conn_id="russina_wiki_clickhouse",
    ds="{{ ds }}",
    dag=dag,
)

delet_data = PostgresOperator(
    task_id="delet_data",
    postgres_conn_id="russian_wiki_postgres",
    sql="""
        DELETE FROM resource.data_views WHERE datetime::date = '{{ds}}';
        """,
    dag=dag,
)

query_telegram >> delet_data
postgresql_to_clickhouse >> delet_data
