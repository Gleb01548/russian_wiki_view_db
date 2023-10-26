import os
import json
import pathlib

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow import AirflowException
from jinja2 import Template


class CreateMessage(BaseOperator):
    """
    Формирует файлы с сообщениями
    """

    def __init__(
        self,
        config: dict,
        global_config: dict,
        path_save: str,
        date_period_type: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.config = config
        self.domain_code = config["domain_code"]
        self.language_resourse = config["resourse"]
        self.languages_target = config["target"]
        self.time_correction = config["time_correction"]
        self.send_massages = config["send_massages"]
        self.path_save = path_save
        self.date_period_type = date_period_type
        self.global_config = global_config

    def _check_args(self):
        if self.date_period_type not in ["day", "week", "month", "year"]:
            raise AirflowException("Неверный тип аргумента date_period_type")

    def _find_actual_date(self, context: Context, time_correction: int):
        actual_date = (
            context["data_interval_start"]
            .add(hours=time_correction)
            .start_of(self.date_period_type)
        )
        return (
            actual_date.format("YYYY-MM-DD"),
            str(actual_date.year),
            str(actual_date.month),
            str(actual_date.day),
            str(actual_date.day_of_week),
        )

    def _pages_data(self, data: dict, translate: bool, stay_in_top: bool):
        pages_data = ""
        for i, page_name in enumerate(data["page_name"]):
            pages_data += "\n"
            if translate:
                page_name = f'{data["page_translation"][i][:50]} | {page_name}'

            if stay_in_top:
                pages_data += f'{data["increment_percent"][i]} | {page_name} | {data["page_view_sum"][i]}'
            else:
                pages_data += (
                    f'{data["rank"][i]} | {page_name} | {data["page_view_sum"][i]}'
                )
        return pages_data

    def _create_save_messages(
        self,
        data: dict,
        config: dict,
        target_config: dict,
        actual_date: str,
        day_of_week: int,
        translate: bool,
        path_save: bool,
        tags: list,
    ) -> None:
        message_settings = target_config["message_settings"]

        pages_data = self._pages_data(
            data=data["actual_data"],
            translate=translate,
            stay_in_top=False,
        )

        if translate:
            col_dict = {
                "col1": message_settings["rank_now"],
                "col2": message_settings["page_name_translate"],
                "col4": message_settings["page_name"],
                "col3": message_settings["sum_views"].removesuffix(" | "),
            }
        else:
            col_dict = {
                "col1": message_settings["rank_now"],
                "col2": message_settings["page_name"],
                "col3": message_settings["sum_views"].removesuffix(" | "),
            }

        message_top_now = Template(message_settings["message_top_now"]).render(
            wikipedia_segment=config["wikipedia_segment"],
            date_period_type=message_settings["date_period_type_translate"][
                self.date_period_type
            ],
            ds=actual_date,
            day_of_week=message_settings["day_of_week_translate"][day_of_week],
            count_views=data["views"]["sum_views_actual"],
            numerical_characteristic=message_settings["numerical_characteristic"][
                self.date_period_type
            ],
            views_increment_percent=data["views"]["views_increment_percent"],
            pages_data=pages_data,
            **col_dict,
        )

        pages_data = self._pages_data(
            data=data["new_in_top"],
            translate=translate,
            stay_in_top=False,
        )

        message_new_pages = Template(message_settings["message_new_pages"]).render(
            wikipedia_segment=config["wikipedia_segment"],
            date_period_type=message_settings["date_period_type_translate"][
                self.date_period_type
            ],
            pages_data=pages_data,
            **col_dict,
        )

        pages_data = self._pages_data(
            data=data["go_out_from_top"],
            translate=translate,
            stay_in_top=False,
        )

        message_go_out_pages = Template(
            message_settings["message_go_out_pages"]
        ).render(
            wikipedia_segment=config["wikipedia_segment"],
            pages_data=pages_data,
            **col_dict,
        )

        pages_data = self._pages_data(
            data=data["stay_in_top"],
            translate=translate,
            stay_in_top=True,
        )

        if translate:
            col_dict = {
                "col1": message_settings["increment_percent"],
                "col2": message_settings["page_name_translate"],
                "col3": message_settings["page_name"],
                "col4": message_settings["sum_views"].removesuffix(" | "),
            }
        else:
            col_dict = {
                "col1": message_settings["increment_percent"],
                "col2": message_settings["page_name"],
                "col3": message_settings["sum_views"].removesuffix(" | "),
            }

        message_difference_pages = Template(
            message_settings["message_difference_pages"]
        ).render(
            wikipedia_segment=config["wikipedia_segment"],
            pages_data=pages_data,
            **col_dict,
        )

        for text, file_type in zip(
            [
                message_top_now,
                message_new_pages,
                message_go_out_pages,
                message_difference_pages,
            ],
            ["TopNow", "NewInTop", "GoOut", "Diff"],
        ):
            text += "\n\n\n"
            text += tags
            text += f" #{message_settings['day_of_week_translate'][day_of_week]}"
            text += f" #period_{message_settings['date_period_type_translate'][self.date_period_type]}"
            text += f" #{file_type}"

            file_name = (
                f"{self.domain_code}_{target_config['domain_code']}_"
                f"{self.date_period_type}_{file_type}_{actual_date}_{self.domain_code}.txt"
            )
            file_save_path = os.path.join(path_save, file_name)

            with open(file_save_path, "w") as f:
                f.write(text)

    def execute(self, context: Context) -> None:
        self._check_args()
        actual_date, year, month, day, day_of_week = self._find_actual_date(
            context, self.time_correction
        )
        path_data = os.path.join(self.path_save, self.domain_code, year)

        mark_to_send_message = False

        if self.send_massages:
            mark_to_send_message = True
            file_data = f"{self.date_period_type}_{actual_date}_{self.domain_code}.json"
            path_load_data = os.path.join(path_data, file_data)

            path_save_message = os.path.join(path_data, "messages", actual_date)
            pathlib.Path(path_save_message).mkdir(parents=True, exist_ok=True)

            tags = " ".join(self.config["tags"])
            for tag, period in zip([year, month, day], ["year", "month", "day"]):
                tags += f" #{self.config['message_settings']['date_period_type_translate'][period]}_{tag}"

            with open(path_load_data) as f:
                data = json.load(f)
            self._create_save_messages(
                data=data,
                config=self.config,
                target_config=self.config,
                actual_date=actual_date,
                day_of_week=int(day_of_week),
                translate=False,
                path_save=path_save_message,
                tags=tags,
            )
            context["ti"].xcom_push(
                key=f"{self.domain_code}_{self.date_period_type}", value=True
            )
            print(
                context["ti"].xcom_pull(
                    task_ids=f"load_to_postgres_trigger_clickhouse_{self.domain_code}.message_day",
                    key="en_day",
                )
            )
        for _, code in self.languages_target:
            target_config = self.global_config[code]
            if target_config["send_massages"]:
                mark_to_send_message = True
                file_data = f"{self.domain_code}_{code}__{self.date_period_type}_{actual_date}_{self.domain_code}.json"
                path_load_data = os.path.join(path_data, "translations", file_data)

                path_data_translation = os.path.join(self.path_save, code, year)
                path_save_message = os.path.join(
                    path_data_translation, "messages", actual_date
                )
                pathlib.Path(path_save_message).mkdir(parents=True, exist_ok=True)

                with open(path_load_data) as f:
                    data = json.load(f)

                tags = " ".join(self.config["tags"])
                for tag, period in zip([year, month, day], ["year", "month", "day"]):
                    tags += f" #{target_config['message_settings']['date_period_type_translate'][period]}_{tag}"

                self._create_save_messages(
                    data=data,
                    config=self.config,
                    target_config=target_config,
                    actual_date=actual_date,
                    day_of_week=int(day_of_week),
                    translate=True,
                    path_save=path_save_message,
                    tags=tags,
                )
        if mark_to_send_message:
            context["ti"].xcom_push(
                key=f"{self.domain_code}_{self.date_period_type}", value=True
            )
