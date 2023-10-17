import logging
import pendulum
from airflow.models import BaseOperator
from airflow.utils.context import Context


class ViewsAnalysis(BaseOperator):
    """
    Подгатавливает скрипты для загрузки данных в postgres
    """

    def __init__(
        self,
        config: dict,
        clickhouse_conn_id: str,
        date_period_start: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.domain_code = config["domain_code"]
        self.time_correction = config["time_correction"]
        self.postgres_conn_id = clickhouse_conn_id
        self.date_period_start = date_period_start

    def _find_actual_date(self, context: Context):
        period_format = {
            "day": "YYYY-MM-DD",
            "month": "YYYY-MM-01",
            "year": "YYYY-01-01",
        }

        return (
            context["data_interval_start"]
            .add(hours=self.time_correction)
            .format(period_format[self.date_period_start])
        )

    def _find_prior_date(self, actual_date: str):
        period_arg = {
            "day": {"days": -1},
            "months": {"month": -1},
            "year": {"years": -1},
        }
        return pendulum.from_format("YYYY-MM-DD").add(
            **period_arg[self.date_period_start]
        )
    
    def _

    def execute(self, context: Context) -> None:
        actual_date = self._find_actual_date(context)
        prior_date = self._find_prior_date(actual_date)
