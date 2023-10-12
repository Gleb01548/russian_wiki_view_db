import logging
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.telegram.hooks.telegram import TelegramHook


class MessegeTelegramNViews(BaseOperator):
    """
    Отправляет через бот сообщение, содержащее N самых читаемых страниц в википедии
    """

    template_fields = ("ds",)

    def __init__(
        self,
        postgres_conn_id,
        telegram_token_bot,
        telegram_chat_id,
        ds="{{ ds }}",
        n_pages=30,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.telegram_token_bot = telegram_token_bot
        self.telegram_chat_id = telegram_chat_id
        self.ds = ds
        self.n_pages = n_pages

    def _get_precessing_data(self) -> str:
        logging.info(f"Расчет {self.n_pages}")
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        query_result = pg_hook.get_records(
            f"""
        select ROW_NUMBER() over(
                order by sum_views DESC
            ),
            page_name,
            sum_views
        from (
                select page_name,
                    datetime::date,
                    SUM(page_view_count) as sum_views
                from resource.data_views
                where datetime::date = '{self.ds}'
                group by page_name,
                    datetime::date
                order by sum_views desc
                limit {self.n_pages}
            ) as table1
            """
        )
        count_views = pg_hook.get_records(
            f"""
            select sum(page_view_count)
            from resource.data_views
            where datetime::date = '{self.ds}'
            """
        )
        count_views = int(count_views[0][0] / 1000)

        message = f"Информация за {self.ds}\nОбщее число просмотров википедии: {count_views} тыс.\nМесто | Имя страницы | Количество просмотров"
        for i in query_result:
            buffer = "\n"
            logging.info(i)
            buffer += " | ".join([str(k) for k in i])
            message += buffer
        logging.info("Текст сообщения удачно сформирован")
        return message

    def execute(self, context) -> None:
        message = self._get_precessing_data()
        tg_hook = TelegramHook(
            token=self.telegram_token_bot, chat_id=self.telegram_chat_id
        )
        tg_hook.send_message({"text": message})
