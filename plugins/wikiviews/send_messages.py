import os
import time

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow import AirflowException
from telebot import TeleBot
from telebot.apihelper import ApiTelegramException


class SendMessages(BaseOperator):
    """
    Отправялет сообщения во все чаты
    """

    def __init__(
        self,
        group_config,
        sub_group_config,
        date_period_type,
        path_save,
        telegram_token_bot,
        chat_ids,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.group_config = group_config
        self.sub_group_config = sub_group_config
        self.date_period_type = date_period_type
        self.path_save = path_save
        self.telegram_token_bot = telegram_token_bot
        self.chat_ids = chat_ids

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
            # str(actual_date.month),
            # str(actual_date.day),
        )

    def _split_text(self, text: str):
        text = text.split("\n")
        len_text = int(len(text) / 2)
        return "\n".join(text[:len_text]), "\n".join(text[len_text:])

    def _send_message(self, chat_id, text):
        try:
            self.tg_bot.send_message(chat_id=chat_id, text=text)
        except ApiTelegramException:
            print("time.sleep")
            time.sleep(30)
            self.tg_bot.send_message(chat_id=chat_id, text=text)

    def _send_photo(self, chat_id, photo):
        try:
            self.tg_bot.send_photo(chat_id=chat_id, photo=photo)
        except ApiTelegramException:
            print("time.sleep")
            time.sleep(30)
            self.tg_bot.send_photo(chat_id=chat_id, photo=photo)

    def send_message(self, path_message, path_graphs, actual_date):
        for message_type in ["TopNow", "NewInTop", "GoOut", "Diff"]:
            file_text_name = (
                f"{self.sub_group_config['domain_code']}_"
                f"{self.group_config['domain_code']}_"
                f"{self.date_period_type}_"
                f"{message_type}_"
                f"{actual_date}_"
                f"{self.sub_group_config['domain_code']}.txt"
            )

            path_text_name = os.path.join(path_message, file_text_name)
            message = open(path_text_name).read()

            for chat_id in self.chat_ids[self.sub_group_config["domain_code"]]:
                chat_id = os.environ.get(chat_id)
                if len(message) < 4096:
                    self._send_message(chat_id=chat_id, text=message)
                else:
                    message_1, message_2 = self._split_text(message)
                    self._send_message(chat_id=chat_id, text=message_1[:4095])
                    self._send_message(chat_id=chat_id, text=message_2[:4095])

                if message_type == "TopNow":
                    file_graphs_name = (
                        "graph_linear_"
                        f"{self.date_period_type}_"
                        f"{actual_date}_"
                        f"{self.sub_group_config['domain_code']}.png"
                    )
                    path_name_graphs = os.path.join(path_graphs, file_graphs_name)
                    self._send_photo(
                        chat_id=chat_id, photo=open(path_name_graphs, "rb")
                    )

    def execute(self, context: Context):
        self._check_args()
        self.tg_bot = TeleBot(token=self.telegram_token_bot)

        actual_date, year = self._find_actual_date(
            context, time_correction=self.sub_group_config["time_correction"]
        )

        path_message = os.path.join(
            self.path_save,
            self.group_config["domain_code"],
            year,
            "messages",
            actual_date,
        )

        path_graphs = os.path.join(
            self.path_save,
            self.sub_group_config["domain_code"],
            year,
            "graphs",
        )

        self.send_message(path_message, path_graphs, actual_date)
