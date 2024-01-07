import os
import time

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow import AirflowException
from telebot import TeleBot


class SendMessages(BaseOperator):
    """
    Отправялет сообщения во все чаты
    """

    def __init__(
        self,
        domain_code,
        date_period_type,
        path_save,
        telegram_token_bot,
        chat_id,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.domain_code = domain_code
        self.date_period_type = date_period_type
        self.path_save = path_save
        self.telegram_token_bot = telegram_token_bot
        self.chat_id = chat_id

    def _check_args(self):
        if self.date_period_type not in ["day", "week", "month", "year"]:
            raise AirflowException("Неверный тип аргумента date_period_type")

    def _find_actual_date(self, context: Context):
        actual_date = context["data_interval_start"].start_of(self.date_period_type)
        return (
            actual_date.format("YYYY-MM-DD"),
            context["data_interval_start"].format("YYYY-MM-DD"),
        )

    def _split_text(self, text: str):
        text = text.split("\n")
        len_text = int(len(text) / 2)
        return "\n".join(text[:len_text]), "\n".join(text[len_text:])

    def _send_message(self, chat_id, text):
        try:
            self.tg_bot.send_message(chat_id=chat_id, text=text)
        except Exception as err:
            print("err", err)
            print("time.sleep")
            time.sleep(30)
            try:
                self.tg_bot.send_message(chat_id=chat_id, text=text)
            except:
                print("time.sleep2")
                time.sleep(30)
                self.tg_bot.send_message(chat_id=chat_id, text=text)

    def _send_document(self, chat_id, path):
        with open(path, "rb") as file:
            try:
                self.tg_bot.send_document(chat_id, file)
            except Exception as err:
                print("err_DOC", err)
                print("time.sleepdoc")
                time.sleep(30)
                try:
                    self.tg_bot.send_document(chat_id, file)
                except:
                    print("time.sleepdoc2")
                    time.sleep(30)
                    self.tg_bot.send_document(chat_id, file)

    def send_message(self, path_message, actual_date, chat_id):
        for message_type in ["TopNow", "NewInTop", "GoOut", "Diff"]:
            file_text_name = f"{self.date_period_type}_{message_type}_{actual_date}.txt"

            path_text_name = os.path.join(path_message, file_text_name)
            message = open(path_text_name).read()

            if len(message) < 4096:
                self._send_message(chat_id=chat_id, text=message)
            else:
                message_1, message_2 = self._split_text(message)
                self._send_message(chat_id=chat_id, text=message_1[:4095])
                self._send_message(chat_id=chat_id, text=message_2[:4095])

    def send_document(self, path_excel, actual_date, chat_id):
        file_doc_name = f"{self.date_period_type}_data_{actual_date}.xlsx"
        path_doc = os.path.join(path_excel, file_doc_name)
        print("path_doc", path_doc)
        self._send_document(chat_id=chat_id, path=path_doc)

    def execute(self, context: Context):
        self._check_args()
        self.tg_bot = TeleBot(token=self.telegram_token_bot)

        actual_date, date = self._find_actual_date(context)

        path_messages = os.path.join(
            self.path_save,
            self.domain_code,
            date,
        )

        path_excel = os.path.join(path_messages, "xlsx")

        self.send_message(
            path_message=path_messages, actual_date=actual_date, chat_id=self.chat_id
        )
        self.send_document(
            path_excel=path_excel, actual_date=actual_date, chat_id=self.chat_id
        )
