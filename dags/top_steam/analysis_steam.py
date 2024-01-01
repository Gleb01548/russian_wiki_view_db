import datetime as dt

import pendulum
from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook


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
    start_date=pendulum.now("UTC").add(days=-2),
    end_date=pendulum.now("UTC"),
    tags=["analysis_steam"],
    default_args=default_args,
)


def _find_new_date(**context):
    data = context["data_interval_start"]
    ph = PostgresHook(postgres_conn_id="wiki_views_postgres")
    result = ph.get_records(
        f"SELECT COUNT(*) FROM steam.steam_data WHERE date = {data}"
    )
    print(result)
    return result[0] > 1


sensor = PythonSensor(task_id="sensor", python_callable=_find_new_date)
