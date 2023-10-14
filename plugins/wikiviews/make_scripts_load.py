from airflow.models import BaseOperator
from airflow.utils.context import Context


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
        with open(self.file_load_path, "r") as f:
            for line in f:
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
                        f" '{actual_time[domain_code]}', '{domain_code}'){symbol}"
                    )
