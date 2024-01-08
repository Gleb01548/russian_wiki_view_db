import os
import pathlib
import pandas as pd

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow import AirflowException
from airflow.utils.context import Context


class PeriodViewsSum(BaseOperator):
    """
    Класс получает данные и сохраняет их json для дальнейшего
    построения графика.
    """

    def __init__(
        self,
        config: dict,
        date_period_type: str,
        conn_id: str,
        path_save: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.domain_code = config["domain_code"]
        self.time_correction = config["time_correction"]
        self.date_period_type = date_period_type
        self.conn_id = conn_id
        self.path_save = path_save

    def _check_args(self):
        if self.date_period_type not in ["day", "week", "month", "year"]:
            raise AirflowException("Неверный тип аргумента date_period_type")

    def _find_actual_date(self, context: Context):
        actual_date = (
            context["data_interval_start"]
            .add(hours=self.time_correction)
            .start_of(self.date_period_type)
        )
        return str(actual_date.year), actual_date.format("YYYY-MM-DD")

    def execute(self, context: Context) -> None:
        self._check_args()
        year, actual_date = self._find_actual_date(context)
        path_save_data = os.path.join(
            self.path_save,
            self.domain_code,
            "data_for_graphs",
            year,
            self.date_period_type,
        )

        pathlib.Path(path_save_data)

        path_save_data = os.path.join(
            path_save_data, f"{actual_date}_data_for_graphs.json"
        )

        conf_graph = {
            "day": {
                "date_func": "HOUR",
                "x_name": "hours",
                "divisor": 10**3,
                "title": f"Number of views for the last day ({actual_date}) in thousands",
            },
            "week": {
                "date_func": "DAYOFWEEK",
                "x_name": "days",
                "divisor": 10**6,
                "title": (
                    "Number of views for the last week "
                    f"""({actual_date})"""
                    " in millions"
                ),
            },
            "month": {
                "date_func": "DAYOFMONTH",
                "x_name": "days",
                "divisor": 10**6,
                "title": (
                    "Number of views for the last month "
                    f"""({actual_date}) in millions"""
                ),
            },
            "year": {
                "date_func": "MONTH",
                "x_name": "months",
                "divisor": 10**6,
                "title": (
                    "Number of views for the last year "
                    f"""({actual_date}) in millions"""
                ),
            },
        }

        conf_graph = conf_graph[self.date_period_type]

        query = f"""
        select {conf_graph['date_func']}(datetime) as {conf_graph['x_name']},
            round(SUM(page_view_count) / {conf_graph["divisor"]}, 2) as views
        from data_views_{self.domain_code}
        where date_trunc('{self.date_period_type}', datetime)
        = '{actual_date}'::datetime
        group by {conf_graph['date_func']}(datetime)
        order by {conf_graph['date_func']}(datetime);
        """

        if self.date_period_type == "day":
            hook = PostgresHook(self.conn_id)
        else:
            hook = ClickHouseHook(clickhouse_conn_id=self.clickhouse_conn_id)

        query_res = hook.execute(query)

        dict_res = {f"{conf_graph['x_name']}": [], "views": []}

        for line in query_res:
            dict_res[conf_graph["x_name"]].append(line[0])
            dict_res["views"].append(line[1])

        df = pd.DataFrame.from_dict(dict_res)
        df.to_json(path_save_data)
