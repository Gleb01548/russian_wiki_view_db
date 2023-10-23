import gzip
from airflow.models import BaseOperator
from airflow.utils.context import Context


class MakeScriptsLoad(BaseOperator):
    """
    Подгатавливает скрипты для загрузки данных в postgres
    """

    template_fields = ("path_dz_file",)

    def __init__(
        self,
        domain_config: dict,
        path_dz_file: str,
        path_save_script: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.domain_config = domain_config
        self.path_dz_file = path_dz_file
        self.path_save_script = path_save_script

    def _xcom_push(self, domain_code, **context):
        for date_period_type in ["day", "week", "month", "year"]:
            context["ti"].xcom_push(
                key=f"{domain_code}_{date_period_type}", value=False
            )

    def execute(self, context: Context) -> None:
        actual_time = {
            domain_code: context["data_interval_start"]
            .add(hours=config["time_correction"])
            .to_atom_string()
            for domain_code, config in self.domain_config.items()
        }
        result = {domain_code: {} for domain_code in self.domain_config.keys()}

        with gzip.open(self.path_dz_file, "r") as f:
            bytes_data = f.read()
            data = bytes_data.decode("utf-8").removesuffix("\n").split("\n")

        for line in data:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code in result:
                result[domain_code][page_title] = view_counts

        for domain_code in result.keys():
            with open(f"{self.path_save_script}_{domain_code}.sql", "w") as f:
                f.write(
                    f"insert into resource.{domain_code} (page_name, page_view_count, datetime) values"
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
            self._xcom_push(domain_code, **context)
