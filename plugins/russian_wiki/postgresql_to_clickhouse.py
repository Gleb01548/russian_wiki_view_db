import logging
from airflow.models import BaseOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresqlToClickhouse(BaseOperator):
    """
    Отправляет через бот сообщение, содержащее N самых читаемых страниц в википедии
    """

    template_fields = ("ds",)

    def __init__(
        self,
        postgres_conn_id,
        clickhouse_conn_id,
        ds="{{ ds }}",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.clickhouse_conn_id = clickhouse_conn_id
        self.ds = ds

    def _query(self) -> str:
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        query_result = pg_hook.get_records(
            f"""
            select page_name,
                page_view_count,
                datetime
            from resource.data_views
            where datetime::date = '{self.ds}'
            """
        )
        logging.info("Выгрузка из sql завершена")
        return query_result

    def execute(self, context) -> None:
        query_result = self._query()
        tg_hook = ClickHouseHook(clickhouse_conn_id=self.clickhouse_conn_id)
        tg_hook.execute(
            "INSERT INTO data_views (page_name, page_view_count, datetime) VALUES",
            query_result,
        )
