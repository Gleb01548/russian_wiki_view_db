import psycopg2
import logging
from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.models.connection import Connection


class LoadPostgres(BaseOperator):
    """
    Подгатавливает скрипты для загрузки данных в postgres
    """

    def __init__(
        self,
        path_script_load_data: str,
        postgres_conn_id: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.path_script_load_data = path_script_load_data

    def execute(self, context: Context) -> None:
        script = open(self.path_script_load_data).read()

        logging.info(f"Запись данных в БД {script[:1000]} и тд.")
        conn_data = Connection().get_connection_from_secrets(self.postgres_conn_id)
        conn = psycopg2.connect(
            dbname=conn_data.schema,
            user=conn_data.login,
            password=conn_data.password,
            host=conn_data.host,
            port=conn_data.port,
        )
        cur = conn.cursor()
        cur.execute(script)
        conn.commit()
        cur.close()
        conn.close()
