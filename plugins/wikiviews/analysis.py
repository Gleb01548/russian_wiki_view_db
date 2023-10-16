import logging
from airflow.models import BaseOperator
from airflow.utils.context import Context


class ViewsAnalysis(BaseOperator):
    """
    Подгатавливает скрипты для загрузки данных в postgres
    """

    def __init__(
        self,
        config: dict,
        postgres_conn_id: str,
        clickhouse_conn_id: str
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.domain_code = config["domain_code"]
        self.time_correction = config["time_correction"]
        self.postgres_conn_id = postgres_conn_id
        self.path_dz_file = path_dz_file
        self.path_script_load_data = path_script_load_data

    def execute(self, context: Context) -> None:
        logging.info(self.path_dz_file)
        actual_date = (
            context["data_interval_start"]
            .add(hours=self.time_correction)
            .to_atom_string()
        )
        result = {}
        with gzip.open(self.path_dz_file, "r") as f:
            bytes_data = f.read()
            data = bytes_data.decode("utf-8").removesuffix("\n").split("\n")

        for line in data:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == self.domain_code:
                result[page_title] = view_counts

        script_list = [
            f"insert into resource.{self.domain_code} (page_name, page_view_count, datetime) values\n",
        ]
        max_index = len(result) - 1
        for index, (pagename, page_view_count) in enumerate(result.items()):
            symbol = ",\n" if max_index > index else ";"
            pagename = pagename.replace("'", "''")
            script_list.append(
                f"('{pagename[:2000]}', '{page_view_count}',"
                f" '{actual_time}'){symbol}"
            )
        script = "\n".join(script_list)
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
