import logging
import pendulum
from airflow.models import BaseOperator
from airflow.utils.context import Context


class Analysis(BaseOperator):
    """
    Подгатавливает скрипты для загрузки данных в postgres
    """

    def __init__(
        self,
        config: dict,
        clickhouse_conn_id: str,
        date_period_type: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.domain_code = config["domain_code"]
        self.time_correction = config["time_correction"]
        self.postgres_conn_id = clickhouse_conn_id
        self.date_period_type = date_period_type

    def _find_actual_date(self, context: Context):
        return (
            context["data_interval_start"]
            .add(hours=self.time_correction)
            .format("YYYY-MM-DD")
        )

    def _find_prior_date(self, actual_date: str):
        period_arg = {
            "day": {"days": -1},
            "week": {"days": -7},
            "months": {"month": -1},
            "year": {"years": -1},
        }
        return pendulum.from_format(actual_date, "YYYY-MM-DD").add(
            **period_arg[self.date_period_type]
        )

    def execute(self, context: Context) -> None:
        actual_date = self._find_actual_date(context)
        prior_date = self._find_prior_date(actual_date)

        subqueries = f"""
        with tab1 as (
            select *
            from analysis.{self.domain_code}
            where date_trunc('month', date) =  date_trunc('month', '2019-12-31'::date)
            and date_period_type = 'month'
        ),

        tab2 as (
            select *
            from analysis.{self.domain_code}
            where date_trunc('month', date) =  date_trunc('month', '2019-12-30'::date)
            and date_period_type = 'month'
        )
                """
