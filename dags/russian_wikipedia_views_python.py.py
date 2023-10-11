import os
import pathlib
from urllib import request
import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "wait_for_downstream": True,
    "retries": 5,
    "retry_delay": dt.timedelta(minutes=3),
    "execution_timeout": dt.timedelta(minutes=60),
}
dir_path = "/data/"
pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)

script_load_res = "load_res.sql"
script_write_table_page_name = "write_table_page_name.sql"
script_write_stat_views = "write_stat_views.sql"
script_delete_res = "delete_res.sql"

file_load_path = os.path.join(dir_path, "wikipageviews.gz")

path_script_load_data = os.path.join(dir_path, script_load_res)
path_script_write_table_page_name = os.path.join(dir_path, script_write_table_page_name)
path_script_write_stat_views = os.path.join(dir_path, script_write_stat_views)
path_script_delete_res = os.path.join(dir_path, script_delete_res)


dag = DAG(
    dag_id="load_to_postgres",
    start_date=dt.datetime(2022, 1, 1),
    end_date=dt.datetime(2022, 1, 2),
    schedule_interval="@hourly",
    tags=["russian_wikipedia", "load_to_postgres"],
    default_args=default_args,
    template_searchpath=dir_path,  # путь для поиска sql-файлов, может быть список
)


def _get_data(output_path, **context):
    year, month, day, hour, *_ = context["data_interval_start"].timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )

    request.urlretrieve(url, output_path)


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={"output_path": file_load_path},
    dag=dag,
)


extract_gz = BashOperator(
    task_id="extract_gz", bash_command=f"gunzip --force {file_load_path}", dag=dag
)


def _fetch_pageviews(
    file_load_path,
    path_script_load_data,
    data_interval_start,
):
    result = {}
    with open(file_load_path, "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "ru":
                result[page_title] = view_counts

    with open(path_script_load_data, "w") as f:
        f.write(
            "insert into resource.data_views (page_name, page_view_count, datetime) values"
        )
        max_index = len(result) - 1
        for index, (pagename, pageviewcount) in enumerate(result.items()):
            symbol = ",\n" if max_index > index else ";"
            pagename = pagename.replace("'", "''")
            pagename = pagename[:1000] if len(pagename) > 1000 else pagename
            f.write(
                f"('{pagename}', '{pageviewcount}', '{data_interval_start}'){symbol}\n"
            )


fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={
        "file_load_path": file_load_path.removesuffix(".gz"),
        "path_script_load_data": path_script_load_data,
    },
    dag=dag,
)

load_to_postgres = PostgresOperator(
    task_id="load_to_postgres",
    postgres_conn_id="russian_wiki_postgres",
    sql=script_load_res,
    dag=dag,
)

# def _make_script_del_res(path_script_delete_res, data_interval_start):
#     with open(path_script_delete_res, "w") as f:
#         f.write(
#             f"""
# DELETE FROM resource.data_views WHERE datetime = '{data_interval_start}';
#             """
#         )


# make_script_del_res = PythonOperator(
#     task_id="make_script_del_res",
#     python_callable=_make_script_del_res,
#     op_kwargs={"path_script_delete_res": path_script_delete_res},
#     dag=dag,
# )


get_data >> extract_gz >> fetch_pageviews >> load_to_postgres
