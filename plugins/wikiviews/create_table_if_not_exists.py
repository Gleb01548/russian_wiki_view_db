import logging
from airflow.models import BaseOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from airflow.models.connection import Connection


class CreateTableIFNotExists(BaseOperator):
    """
    Создает таблицы, если таковых не существует
    """

    def __init__(
        self,
        config: dict,
        postgres_conn_id: str,
        clickhouse_conn_id: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.domain_code = config["domain_code"]
        self.postgres_conn_id = postgres_conn_id
        self.clickhouse_conn_id = clickhouse_conn_id

    def _postgres(self) -> None:
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        pg_hook.get_records(
            f"""
            drop table if exists resource.{self.domain_code};
            
            create schema if not exists resource;
            create table if not exists resource.{self.domain_code} (
                {self.domain_code}_id serial primary key,
                page_name VARCHAR(2000) not null,
                page_view_count int not null,
                datetime TIMESTAMP not null,
                constraint {self.domain_code}_unique_page_name_datetion unique (page_name, datetime)
            );
            ALTER SEQUENCE resource.{self.domain_code}_{self.domain_code}_id_seq CYCLE;
            """  # noqa
        )

        pg_hook.get_records(
            f"""
            drop table if exists analysis.{self.domain_code};
            
            create schema if not exists analysis;
            create table if not exists analysis.{self.domain_code} (
                {self.domain_code}_id serial primary key,
                rank int not null,
                page_name VARCHAR(2000) not null,
                page_view_sum int not null,
                date date not null,
                date_period_type VARCHAR(50) not null,
                constraint {self.domain_code}_unique_page_name_datetion unique (page_name, date, date_period_type)
            );
            """  # noqa
        )

        pg_hook.get_records(
            f"""
            drop table if exists sum_views.{self.domain_code};
            
            create schema if not exists sum_views;
            create table if not exists sum_views.{self.domain_code} (
                {self.domain_code}_id serial primary key,
                page_view_sum int not null,
                date date not null,
                date_period_type VARCHAR(50) not null,
                constraint {self.domain_code}_unique_datetime_date_period_start unique (date, date_period_type)
            );
            """  # noqa
        )
        logging.info("Создание таблиц postgres завершено")

    def _clickhouse(self) -> None:
        ch_hook = ClickHouseHook(clickhouse_conn_id=self.clickhouse_conn_id)

        ch_hook.execute(f"drop table if exists data_views_{self.domain_code};")
        ch_hook.execute(
            f"""
            create table if not exists data_views_{self.domain_code} (
            page_name String,
            page_view_count Int32,
            datetime Date
            ) ENGINE = MergeTree ORDER BY (datetime, page_name);
            """
        )

        conn_data = Connection().get_connection_from_secrets(self.postgres_conn_id)

        dbname = conn_data.schema
        user = conn_data.login
        password = conn_data.password
        host = conn_data.host
        port = conn_data.port

        ch_hook.execute(f"drop table if exists postgres_resource_{self.domain_code};")
        ch_hook.execute(
            f"""
            create table if not exists postgres_resource_{self.domain_code} (
            page_name String,
            page_view_count Int32,
            datetime Date
            ) ENGINE = PostgreSQL('{host}:{port}', '{dbname}',
            '{self.domain_code}', '{user}', '{password}', 'resource');
            """
        )

        ch_hook.execute(f"drop table if exists postgres_analysis_{self.domain_code};")
        ch_hook.execute(
            f"""
            create table if not exists postgres_analysis_{self.domain_code} (
            rank Int32,
            page_name String,
            page_view_sum Int32,
            date Date,
            date_period_type String
            ) ENGINE = PostgreSQL('{host}:{port}', '{dbname}',
            '{self.domain_code}', '{user}', '{password}', 'analysis');
            """
        )

        ch_hook.execute(f"drop table if exists postgres_sum_views_{self.domain_code};")
        ch_hook.execute(
            f"""
            create table if not exists postgres_sum_views_{self.domain_code} (
            page_view_sum Int32,
            date Date,
            date_period_type String
            ) ENGINE = PostgreSQL('{host}:{port}', '{dbname}',
            '{self.domain_code}', '{user}', '{password}', 'sum_views');
            """
        )

    def execute(self, context: Context) -> None:
        self._postgres()
        self._clickhouse()
