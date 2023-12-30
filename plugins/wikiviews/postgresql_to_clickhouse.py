import logging
from airflow.models import BaseOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.utils.context import Context


class PostgresqlToClickhouse(BaseOperator):
    """
    Отправляет через бот сообщение, содержащее N самых читаемых страниц в википедии
    """

    def __init__(
        self,
        config: dict,
        clickhouse_conn_id: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.time_correction = config["time_correction"]
        self.domain_code = config["domain_code"]
        self.clickhouse_conn_id = clickhouse_conn_id

    def execute(self, context: Context) -> None:
        logging.info(context["data_interval_start"])
        actual_date = (
            context["data_interval_start"]
            .add(hours=self.time_correction)
            .to_date_string()
        )
        tg_hook = ClickHouseHook(clickhouse_conn_id=self.clickhouse_conn_id)
        tg_hook.execute(
            f"""
            INSERT INTO data_views_{self.domain_code} (page_name, page_view_count, datetime)
            select page_name,
                page_view_count,
                datetime
            from postgres_resource_{self.domain_code}
            where datetime::date = '{actual_date}'
            """
        )
