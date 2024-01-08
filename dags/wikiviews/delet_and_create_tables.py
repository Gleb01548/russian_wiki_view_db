import datetime as dt

import pendulum
from airflow import DAG
from airflow.decorators import task_group
from wikiviews.create_table_if_not_exists import CreateTableIFNotExists
from airflow.operators.empty import EmptyOperator


from constants import (
    PATH_WORK_FILES,
    DOMAIN_CONFIG,
    PASTGRES_CONN_ID,
    CLICKHOUSE_CONN_ID,
)

default_args = {
    "wait_for_downstream": True,
    "retries": 10,
    "retry_delay": dt.timedelta(seconds=10),
    "execution_timeout": dt.timedelta(minutes=60),
    # "end_date": pendulum.now("UTC"),
}

dag = DAG(
    dag_id="drop_and_create_tables",
    tags=[
        "test",
        "wikipedia_views",
        "drop_table",
        "create_table",
    ],
    default_args=default_args,
    start_date=pendulum.now("UTC"),
    schedule_interval=None,
    template_searchpath=PATH_WORK_FILES,
)


@task_group(dag=dag, group_id="create_table")
def create_table_group():
    for domain_code, config in DOMAIN_CONFIG.items():
        create_table = CreateTableIFNotExists(
            task_id=f"create_table_{domain_code}",
            config=config,
            postgres_conn_id=PASTGRES_CONN_ID,
            clickhouse_conn_id=CLICKHOUSE_CONN_ID,
            dag=dag,
        )
        create_table


create_table_group = create_table_group()

create_table_group
