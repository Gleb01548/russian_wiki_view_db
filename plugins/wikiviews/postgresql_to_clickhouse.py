import logging
from airflow.models import BaseOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context


class PostgresqlToClickhouse(BaseOperator):
    """
    Отправляет через бот сообщение, содержащее N самых читаемых страниц в википедии
    """

    def __init__(
        self,
        config: dict,
        postgres_conn_id: str,
        clickhouse_conn_id: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.time_correction = config["time_correction"]
        self.domain_code = config["domain_code"]
        self.postgres_conn_id = postgres_conn_id
        self.clickhouse_conn_id = clickhouse_conn_id

    def _query(self) -> str:
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        query_result = pg_hook.get_records(
            f"""
            select page_name,
                page_view_count,
                datetime
            from resource.{self.domain_code}
            where datetime::date = '{self.actual_date}'
            """
        )
        logging.info("Выгрузка из sql завершена")
        return query_result

    def execute(self, context: Context) -> None:
        self.actual_date = (
            context["data_interval_start"]
            .add(hours=self.time_correction)
            .to_date_string()
        )
        query_result = self._query()
        tg_hook = ClickHouseHook(clickhouse_conn_id=self.clickhouse_conn_id)
        tg_hook.execute(
            f"INSERT INTO data_views_{self.domain_code} (page_name, page_view_count, datetime) VALUES",
            query_result,
        )
