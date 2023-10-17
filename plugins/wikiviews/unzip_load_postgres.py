import gzip
import psycopg2
import logging
from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.models.connection import Connection


class UnzipLoadPostgres(BaseOperator):
    """
    Подгатавливает скрипты для загрузки данных в postgres
    """

    template_fields = ("path_dz_file",)

    def __init__(
        self,
        config: dict,
        path_dz_file: str,
        path_script_load_data: str,
        postgres_conn_id: str,
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
        actual_time = (
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


class MakeScriptsLoad(BaseOperator):
    """
    Подгатавливает скрипты для загрузки данных в postgres
    """

    template_fields = ("file_load_path",)

    def __init__(
        self,
        domain_code_time_correct: dict,
        file_load_path: str,
        path_script_load_data: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.domain_code_time_correct = domain_code_time_correct
        self.file_load_path = file_load_path
        self.path_script_load_data = path_script_load_data

    def execute(self, context: Context) -> None:
        actual_time = {
            domain_code: context["data_interval_start"]
            .add(hours=time_correction)
            .to_atom_string()
            for domain_code, time_correction in self.domain_code_time_correct.items()
        }
        result = {
            domain_code: {} for domain_code in self.domain_code_time_correct.keys()
        }

        with gzip.open(self.path_dz_file, "r") as f:
            bytes_data = f.read()
            data = bytes_data.decode("utf-8").removesuffix("\n").split("\n")

        for line in data:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code in result:
                result[domain_code][page_title] = view_counts

        for domain_code in result.keys():
            with open(f"{self.path_script_load_data}_{domain_code}{'.sql'}", "w") as f:
                f.write(
                    "insert into resource.data_views (page_name, page_view_count, datetime, domain_code) values"
                )
                max_index = len(result[domain_code]) - 1
                for index, (pagename, page_view_count) in enumerate(
                    result[domain_code].items()
                ):
                    symbol = ",\n" if max_index > index else ";"
                    pagename = pagename.replace("'", "''")
                    f.write(
                        f"('{pagename[:2000]}', '{page_view_count}',"
                        f" '{actual_time[domain_code]}'){symbol}"
                    )
