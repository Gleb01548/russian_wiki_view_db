import os

import logging
import json
import pathlib
import pendulum
import pandas as pd
import plotly.express as px
from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow import AirflowException


class Analysis(BaseOperator):
    """
    Подгатавливает скрипты для загрузки данных в postgres
    """

    def __init__(
        self,
        config: dict,
        postgres_conn_id: str,
        clickhouse_conn_id: str,
        date_period_type: str,
        path_save: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.domain_code = config["domain_code"]
        self.time_correction = config["time_correction"]
        self.postgres_conn_id = postgres_conn_id
        self.clickhouse_conn_id = clickhouse_conn_id
        self.date_period_type = date_period_type
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

    def _find_prior_date(self, actual_date: str):
        period_arg = {
            "day": {"days": -1},
            "week": {"days": -7},
            "month": {"months": -1},
            "year": {"years": -1},
        }
        return (
            pendulum.from_format(actual_date, "YYYY-MM-DD")
            .add(**period_arg[self.date_period_type])
            .start_of(self.date_period_type)
            .format("YYYY-MM-DD")
        )

    def _create_graphs(self, actual_date: str, path_save: str):
        path_save = os.path.join(path_save, "graphs")
        pathlib.Path(path_save).mkdir(parents=True, exist_ok=True)

        path_save_graph = os.path.join(
            path_save,
            f"graph_linear_{self.date_period_type}_{actual_date}_{self.domain_code}.png",
        )

        logging.info(actual_date)

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

        ch_hook = ClickHouseHook(clickhouse_conn_id=self.clickhouse_conn_id)
        query_res = ch_hook.execute(query)

        dict_res = {f"{conf_graph['x_name']}": [], "views": []}

        for line in query_res:
            dict_res[conf_graph["x_name"]].append(line[0])
            dict_res["views"].append(line[1])

        df = pd.DataFrame.from_dict(dict_res)

        fig = px.line(df, x=conf_graph["x_name"], y="views", template="plotly_dark")
        fig.update_layout(
            title=dict(
                text=conf_graph["title"],
                x=0.5,
                font=dict(size=23),
            ),
            xaxis=dict(tickfont=dict(size=22), titlefont=dict(size=22)),
            yaxis=dict(tickfont=dict(size=22), titlefont=dict(size=22)),
        )
        fig.write_image(path_save_graph)

    def execute(self, context: Context) -> None:
        self._check_args()
        year, actual_date = self._find_actual_date(context)
        prior_date = self._find_prior_date(actual_date)

        path_save = os.path.join(
            self.path_save,
            self.domain_code,
            year,
        )
        pathlib.Path(path_save).mkdir(parents=True, exist_ok=True)

        path_save_data = os.path.join(
            path_save,
            f"{self.date_period_type}_{actual_date}_{self.domain_code}.json",
        )

        query_actual_data = f"""
            select rank,
            page_name,
            page_view_sum
            from analysis.{self.domain_code}
            where date_trunc('{self.date_period_type}', date)
             = '{actual_date}'::date
            and date_period_type = '{self.date_period_type}'
        """

        subqueries = f"""
            with tab1 as (
            select *
            from analysis.{self.domain_code}
            where date_trunc('{self.date_period_type}', date)
             = '{actual_date}'::date
            and date_period_type = '{self.date_period_type}'
            ),

            tab2 as (
            select *
            from analysis.{self.domain_code}
            where date_trunc('{self.date_period_type}', date)
             = '{prior_date}'::date
            and date_period_type = '{self.date_period_type}'
            )
            """
        # запрос страниц которые вошли топ
        query_new_in_top = f"""
        {subqueries}
        
        select t1.rank, t1.page_name, t1.page_view_sum
        from tab1 as t1
        full join tab2 t2 on t1.page_name = t2.page_name
        where t2.page_name is null
        order by t1.rank
        """  # noqa

        # запрос страниц, которых раньше не было в топе
        query_go_out_from_top = f""" 
        {subqueries}
                           
        select t2.rank, t2.page_name, t2.page_view_sum
        from tab1 as t1
        full join tab2 t2 on t1.page_name = t2.page_name
        where t1.page_name is null
        order by t2.rank
        """  # noqa

        # запрос страниц, которые остались в топе
        query_stay_in_top = f""" 
        {subqueries}
                           
        select
        CASE
            WHEN increment_percent < 0 THEN '⬇'
            WHEN increment_percent > 0 THEN '⬆'
            ELSE '↔'
        END as icon,
                page_name,
                rank_actual,
                rank_past,
                ROUND(increment_percent::numeric, 2)::float,
                page_view_sum
        from (
        select t1.page_name as page_name, 
        t1.rank as rank_actual, 
        t2.rank as rank_past,
        (t1.page_view_sum - t2.page_view_sum)::float/t2.page_view_sum * 100 as increment_percent, 
        t1.page_view_sum as page_view_sum
        from tab1 as t1
        inner join tab2 t2 on t1.page_name = t2.page_name
        order by t1.rank) as tab3
        order by ROUND(increment_percent::numeric, 2)::float DESC
        """  # noqa

        query_sum_views_actual = f"""
        select page_view_sum
        from sum_views.{self.domain_code}
        where date_trunc('{self.date_period_type}', date)
            = '{actual_date}'::date
        and date_period_type = '{self.date_period_type}'
        """

        query_sum_views_prior = f"""
        select page_view_sum
        from sum_views.{self.domain_code}
        where date_trunc('{self.date_period_type}', date)
            = '{prior_date}'::date
        and date_period_type = '{self.date_period_type}'
        """

        data = {
            "views": {"sum_views_actual": None, "views_increment_percent": None},
            "actual_data": {"rank": [], "page_name": [], "page_view_sum": []},
            "new_in_top": {"rank": [], "page_name": [], "page_view_sum": []},
            "go_out_from_top": {"rank": [], "page_name": [], "page_view_sum": []},
            "stay_in_top": {
                "icon": [],
                "page_name": [],
                "rank_actual": [],
                "rank_past": [],
                "increment_percent": [],
                "page_view_sum": [],
            },
        }

        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        actual_data = pg_hook.get_records(query_actual_data)
        new_in_top = pg_hook.get_records(query_new_in_top)
        go_out_from_top = pg_hook.get_records(query_go_out_from_top)
        stay_in_top = pg_hook.get_records(query_stay_in_top)

        sum_views_actual = int(pg_hook.get_records(query_sum_views_actual)[0][0])
        sum_views_prior = pg_hook.get_records(query_sum_views_prior)

        if sum_views_prior:
            sum_views_prior = int(pg_hook.get_records(query_sum_views_prior)[0][0])
            views_increment_percent = round(
                (sum_views_actual - sum_views_prior) / sum_views_prior * 100, 2
            )
        else:
            views_increment_percent = None
        if self.date_period_type == "day":
            degree = 3
        else:
            degree = 6

        data["views"]["sum_views_actual"] = round(sum_views_actual / 10**degree, 2)
        data["views"]["views_increment_percent"] = views_increment_percent

        for row in actual_data:
            for value, key in zip(row, ["rank", "page_name", "page_view_sum"]):
                data["actual_data"][key].append(value)

        for row in new_in_top:
            for value, key in zip(row, ["rank", "page_name", "page_view_sum"]):
                data["new_in_top"][key].append(value)

        for row in go_out_from_top:
            for value, key in zip(row, ["rank", "page_name", "page_view_sum"]):
                data["go_out_from_top"][key].append(value)

        for row in stay_in_top:
            for value, key in zip(
                row,
                [
                    "icon",
                    "page_name",
                    "rank_actual",
                    "rank_past",
                    "increment_percent",
                    "page_view_sum",
                ],
            ):
                data["stay_in_top"][key].append(value)

        with open(path_save_data, "w") as f:
            json.dump(data, f, ensure_ascii=False)

        self._create_graphs(actual_date=actual_date, path_save=path_save)
