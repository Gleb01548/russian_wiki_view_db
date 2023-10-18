import logging
import pendulum
from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow import AirflowException


class АggregationLoadPostgres(BaseOperator):
    """
    Подгатавливает скрипты для загрузки данных в postgres
    """

    def __init__(
        self,
        config: dict,
        clickhouse_conn_id: str,
        date_period_type: str,
        query_type: str,
        n_pages: int = 100,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.domain_code = config["domain_code"]
        self.time_correction = config["time_correction"]
        self.clickhouse_conn_id = clickhouse_conn_id
        self.date_period_type = date_period_type
        self.query_type = query_type
        self.n_pages = n_pages

        self._check_args()

    def _check_args(self):
        if self.date_period_type not in ["day", "week", "month", "year"]:
            raise AirflowException("Неверный тип аргумента date_period_type")

        if self.query_type not in ["aggregate_date", "sum_views"]:
            raise AirflowException("Неверный тип аргумента query_type")

    def _find_actual_date(self, context: Context):
        # period_format = {
        #     "day": "YYYY-MM-DD",
        #     "week": "YYYY-MM-DD",
        #     "month": "YYYY-MM-01",
        #     "year": "YYYY-01-01",
        # }

        return (
            context["data_interval_start"]
            .add(hours=self.time_correction)
            .format("YYYY-MM-DD")
        )

    def _find_prior(self, actual_date):
        return pendulum.from_format(actual_date, "YYYY-MM-DD").add(days=-6)

    def execute(self, context: Context) -> None:
        actual_date = self._find_actual_date(context)
        # if self.date_period_type == "week":
        #     prior_date = self._find_prior(actual_date)
        # else:
        #     prior_date = None

        # if prior_date:
        #     condition = f"BETWEEN '{prior_date}' AND '{actual_date}'"
        #     date_for_sum_views = prior_date
        # else:
        #     condition = f"= '{actual_date}'"
        #     date_for_sum_views = actual_date

        logging.info(actual_date)

        tg_hook = ClickHouseHook(clickhouse_conn_id=self.clickhouse_conn_id)

        if self.query_type == "aggregate_date":
            script = f"""
                INSERT INTO postgres_analysis_{self.domain_code}
                 (rank, page_name, page_view_sum, date, date_period_type)
                 select row_number() OVER(ORDER BY page_view_sum DESC) as rank,
                 page_name,
                 page_view_sum,
                 date,
                 '{self.date_period_type}'
                 from  (
                        select page_name,
                        SUM(page_view_count) as page_view_sum,
                        date_trunc('{self.date_period_type}', datetime) as date
                        from data_views_{self.domain_code}
                        where date_trunc('{self.date_period_type}', datetime) =
                        date_trunc('{self.date_period_type}', '{actual_date}'::date)
                        group by page_name, date_trunc('{self.date_period_type}', datetime)
                        order by page_view_sum desc
                        limit {self.n_pages}
                ) as tb1;
                """
        elif self.query_type == "sum_views":
            script = f"""
                INSERT INTO postgres_sum_views_{self.domain_code}
                 (page_view_sum, date, date_period_type)
                 select sum(page_view_count) as page_view_sum,
                        DATE_TRUNC('{self.date_period_type}', datetime),
                        '{self.date_period_type}'
                 from data_views_{self.domain_code}
                 where date_trunc('{self.date_period_type}', datetime) =
                 date_trunc('{self.date_period_type}', '{actual_date}'::date)
                 group by  date_trunc('{self.date_period_type}', datetime);
                """

        tg_hook.execute(script)
