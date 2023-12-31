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

    def _check_args(self):
        if self.date_period_type not in ["day", "week", "month", "year"]:
            raise AirflowException("Неверный тип аргумента date_period_type")

        if self.query_type not in ["aggregate_date", "sum_views"]:
            raise AirflowException("Неверный тип аргумента query_type")

    def _find_actual_date(self, context: Context):
        actual_date = (
            context["data_interval_start"]
            .add(hours=self.time_correction)
            .start_of(self.date_period_type)
        )

        return actual_date.format("YYYY-MM-DD")

    def _find_prior(self, actual_date):
        return pendulum.from_format(actual_date, "YYYY-MM-DD").add(days=-6)

    def execute(self, context: Context) -> None:
        self._check_args()
        actual_date = self._find_actual_date(context)
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
                        '{actual_date}'::date as date
                        from data_views_{self.domain_code}
                        where date_trunc('{self.date_period_type}', datetime) =
                        '{actual_date}'::date
                        group by page_name, '{actual_date}'::date
                        order by page_view_sum desc
                        limit {self.n_pages}
                ) as tb1;
                """
        elif self.query_type == "sum_views":
            script = f"""
                INSERT INTO postgres_sum_views_{self.domain_code}
                 (page_view_sum, date, date_period_type)
                 select sum(page_view_count) as page_view_sum,
                        '{actual_date}'::date as date,
                        '{self.date_period_type}'
                 from data_views_{self.domain_code}
                 where date_trunc('{self.date_period_type}', datetime) =
                 '{actual_date}'::date
                 group by '{actual_date}'::date;
                """

        tg_hook.execute(script)
