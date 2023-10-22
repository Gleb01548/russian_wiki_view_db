import os
import json
import requests
import pathlib

import pendulum
from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow import AirflowException


class Translate(BaseOperator):
    """
    переводит названия страниц
    """

    def __init__(
        self,
        config: dict,
        date_period_type: str,
        path_save: str,
        path_load_data: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.domain_code = config["domain_code"]
        self.time_correction = config["time_correction"]
        self.language_resourse = config["resourse"]
        self.languages_target = config["target"]
        self.date_period_type = date_period_type
        self.path_save = path_save
        self.path_load_data = path_load_data

    def _check_args(self):
        if self.date_period_type not in ["day", "week", "month", "year"]:
            raise AirflowException("Неверный тип аргумента date_period_type")

    def _find_actual_date(self, context: Context):
        return (
            context["data_interval_start"]
            .add(hours=self.time_correction)
            .start_of(self.date_period_type)
            .format("YYYY-MM-DD")
        )

    def _translate_and_save_data(
        self, batch: list, list_len: list, file_load_name: str, data: dict, year: int
    ):
        url = "http://192.168.0.124:8239/translate"

        for target, code in self.languages_target:
            file_save_name = f"{self.domain_code}_{code}__{file_load_name}"
            path_save_data = os.path.join(
                self.path_save, self.domain_code, year, "translations"
            )
            pathlib.Path(path_save_data).mkdir(parents=True, exist_ok=True)
            path_save_data = os.path.join(path_save_data, file_save_name)

            data_requests = {
                "source": self.language_resourse,
                "target": target,
                "sentences": batch,
            }
            response = requests.post(url, data=data_requests, timeout=60 * 20)

            translation = response.json()

            translation_lists = {}

            k = 0
            k1 = 0
            for i, old_list in zip(
                list_len,
                ["actual_data", "new_in_top", "go_out_from_top", "stay_in_top"],
            ):
                k1 += i
                translation_lists[old_list] = translation[k:k1]
                k = k1

            for key, value in translation_lists.items():
                data[key]["page_translation"] = value

            with open(path_save_data, "w") as f:
                json.dump(data, f, ensure_ascii=False)

    def execute(self, context: Context) -> None:
        self._check_args()
        actual_date = self._find_actual_date(context)
        year = str(pendulum.from_format(actual_date, "YYYY-MM-DD").year)

        file_load_name = (
            f"{self.date_period_type}_{actual_date}_{self.domain_code}.json"
        )

        path_load_file = os.path.join(
            self.path_load_data,
            self.domain_code,
            year,
            file_load_name,
        )
        with open(path_load_file) as f:
            data = json.load(f)

        actual_data = data["actual_data"]["page_name"]
        new_in_top = data["new_in_top"]["page_name"]
        go_out_from_top = data["go_out_from_top"]["page_name"]
        stay_in_top = data["stay_in_top"]["page_name"]

        batch = []
        list_len = []
        for i in [actual_data, new_in_top, go_out_from_top, stay_in_top]:
            batch.extend(i)
            list_len.append(len(i))

        self._translate_and_save_data(
            batch=batch,
            list_len=list_len,
            file_load_name=file_load_name,
            data=data,
            year=year,
        )
