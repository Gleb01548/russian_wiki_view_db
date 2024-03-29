import os
import json
import pathlib

import pendulum
import pandas as pd
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow import AirflowException
from airflow.utils.context import Context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base_hook import BaseHook


class Analysis(BaseOperator):
    """
    Подгатавливает скрипты для загрузки данных в postgres
    """

    def __init__(
        self,
        postgres_conn_id: str,
        path_save: str,
        date_period_type: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.path_save = path_save
        self.date_period_type = date_period_type

    def _check_args(self) -> None:
        if self.date_period_type not in ["day", "week", "month", "year"]:
            raise AirflowException("Неверный тип аргумента date_period_type")

    def _find_actual_date(self, context: Context) -> str:
        actual_date = context["data_interval_start"].start_of(self.date_period_type)
        self.year_for_cum = (
            context["data_interval_start"].start_of("year").format("YYYY-MM-DD")
        )
        self.day_for_cum = (
            context["data_interval_start"].start_of("day").format("YYYY-MM-DD")
        )
        return actual_date.format("YYYY-MM-DD")

    def _find_prior_date(self, actual_date: str) -> str:
        period_arg = {
            "day": {"days": -1},
            "week": {"days": -7},
            "month": {"months": -1},
            "year": {"years": -1},
        }
        return (
            pendulum.from_format(actual_date, "YYYY-MM-DD")
            .add(**period_arg[self.date_period_type])
            .start_of(self.date_period_type)
            .format("YYYY-MM-DD")
        )

    def _cummulativ_total_for_year(self):
        return f"""
        select row_number() OVER(order by top_for_period DESC, avg_for_period DESC) as rank,
        game_name,
        top_for_period, 
        avg_for_period
        from (select game_name, 
                     COUNT(*) as top_for_period,
                     AVG(players_count)::float as avg_for_period
              from (select * 
                    from steam.steam_data 
                    where date_trunc('year', date) = '{self.year_for_cum}' and date <= '{self.day_for_cum}') as tab1
              group by game_name
              ) as tab2
        """

    def new_go_out(
        self,
        query_new_in_top: str,
        query_go_out_from_top: str,
        columns_new: list,
        columns_go_out: list,
    ) -> (tuple, tuple):
        conn = BaseHook.get_connection(self.postgres_conn_id)
        host = conn.host
        db = conn.schema
        login = conn.login
        password = conn.password
        engine = create_engine(f"postgresql+psycopg2://{login}:{password}@{host}/{db}")

        df1 = pd.read_sql(sql=query_new_in_top, con=engine)
        df1 = df1[columns_new]

        df2 = pd.read_sql(sql=query_go_out_from_top, con=engine)
        df2 = df2[columns_go_out]

        return df1.to_numpy(), df2.to_numpy()

    def _analysis_day(
        self,
        actual_date: str,
        prior_date: str,
    ) -> dict:
        # запрос текущего топа
        query_actual_data = f"""
        select row_number() OVER(order by players_count DESC) as rank,
        game_name,
        players_count
        from steam.steam_data
        where date = '{actual_date}'::date
        """

        subqueries = f"""
        with tab1 as (
        select row_number() OVER(order by players_count DESC) as rank,
        game_name,
        players_count
        from steam.steam_data
        where date = '{actual_date}'::date  
        ),
        
        tab2 as (
        select row_number() OVER(order by players_count DESC) as rank2,
        game_name as game_name2,
        players_count as players_count2
        from steam.steam_data
        where date = '{prior_date}'::date  
        )
        """

        # запрос игр, которые вошли в топ
        query_new_in_top = f"""
        {subqueries}
        
        select *
        from tab1 as t1
        full join tab2 t2 on t1.game_name = t2.game_name2
        where t2.game_name2 is null
        order by t1.rank
        """

        # запрос игр, которые из топа вышли
        query_go_out_from_top = f"""
        {subqueries}
        
        select *
        from tab2 as t2
        full join tab1 t1 on t2.game_name2 = t1.game_name
        where t1.game_name is null
        order by t2.rank2
        """

        query_stay_in_top = f"""
        {subqueries}
        
        select 
        increment_percent_players, 
        game_name, 
        players_count,
        rank_actual,
        rank_prior
        from (
            select t1.game_name as game_name,
                   t1.rank as rank_actual, 
                   t2.rank2 as rank_prior,
                   (t1.players_count - t2.players_count2)::float/t2.players_count2 * 100 as increment_percent_players,
                   t1.players_count as players_count  
            from tab1 as t1
            inner join tab2 as t2 on t1.game_name = t2.game_name2            
        ) as t3
        order by ROUND(increment_percent_players::numeric, 2)::float DESC
        """

        actual_data = self.pg_hook.get_records(query_actual_data)
        new_in_top, go_out_from_top = self.new_go_out(
            query_new_in_top=query_new_in_top,
            query_go_out_from_top=query_go_out_from_top,
            columns_new=["rank", "game_name", "players_count"],
            columns_go_out=["rank2", "game_name2", "players_count2"],
        )
        stay_in_top = self.pg_hook.get_records(query_stay_in_top)
        cummulativ_total_for_year = self.pg_hook.get_records(
            self._cummulativ_total_for_year()
        )

        return {
            "actual_data": actual_data,
            "new_in_top": new_in_top,
            "go_out_from_top": go_out_from_top,
            "stay_in_top": stay_in_top,
            "cummulativ_total_for_year": cummulativ_total_for_year,
        }

    def _analysis_not_day(
        self, actual_date: str, prior_date: str, date_period_type: str
    ) -> dict:
        # запрос текущего топа
        query_actual_data = f"""
        select row_number() OVER(order by top_for_period DESC, avg_for_period DESC) as rank,
        game_name,
        top_for_period, 
        avg_for_period
        from (select game_name, 
                     COUNT(*) as top_for_period,
                     AVG(players_count)::float as avg_for_period
              from (select * 
                    from steam.steam_data 
                    where date_trunc('{date_period_type}', date) = '{actual_date}') as tab1
              group by game_name
              ) as tab2
        limit 100
        """

        ## аггрегация по всем данным
        query_actual_data_full = f"""
        select row_number() OVER(order by top_for_period DESC, avg_for_period DESC) as rank,
        game_name,
        top_for_period, 
        avg_for_period
        from (select game_name, 
                     COUNT(*) as top_for_period,
                     AVG(players_count)::float as avg_for_period
              from (select * 
                    from steam.steam_data 
                    where date_trunc('{date_period_type}', date) = '{actual_date}') as tab1
              group by game_name
              ) as tab2
        """

        subqueries = f"""
        with tab1 as (
        select row_number() OVER(order by top_for_period DESC, avg_for_period DESC) as rank,
        game_name,
        top_for_period, 
        avg_for_period
        from (select game_name, 
                     COUNT(*) as top_for_period,
                     AVG(players_count)::float as avg_for_period
              from (select * 
                    from steam.steam_data 
                    where date_trunc('{date_period_type}', date) = '{actual_date}') as tab1
              group by game_name
              ) as tab2
        limit 100
        ),
        
        tab2 as (
        select row_number() OVER(order by top_for_period2 DESC, avg_for_period2 desc) as rank2,
        game_name2,
        top_for_period2, 
        avg_for_period2
        from (select game_name as game_name2, 
                     COUNT(*) as top_for_period2,
                     AVG(players_count)::float as avg_for_period2
              from (select * 
                    from steam.steam_data 
                    where date_trunc('{date_period_type}', date) = '{prior_date}') as tab1
              group by game_name
              ) as tab2
        limit 100
        )
        """

        # запрос игр, которые вошли в текущем периоде
        query_new_in_top = f"""
        {subqueries}
        
        select *
        from tab1 as t1
        full join tab2 t2 on t1.game_name = t2.game_name2
        where t2.game_name2 is null
        order by t1.rank
        """

        # запрос игр, которые вышли из топа в текущем периоде
        query_go_out_from_top = f"""
        {subqueries}
        
        select *
        from tab2 as t2
        full join tab1 t1 on t2.game_name2 = t1.game_name
        where t1.game_name is null
        order by t2.rank2
        """
        query_stay_in_top = f"""
        {subqueries}
        
        select 
        increment_days_in_top,
        increment_percent_avg_players::float, 
        game_name,
        top_for_period_actual,
        top_for_period_prior,
        avg_for_period_actual,
        avg_for_period_prior,
        rank_actual,
        rank_prior
        from (
            select 
                   (t1.top_for_period - t2.top_for_period2)::int as increment_days_in_top,
                   (t1.avg_for_period - t2.avg_for_period2)::float/t2.avg_for_period2 * 100 as increment_percent_avg_players,
                   t1.game_name as game_name,
                   t1.top_for_period as top_for_period_actual, 
                   t2.top_for_period2 as top_for_period_prior,
                   t1.avg_for_period as avg_for_period_actual,
                   t2.avg_for_period2 as avg_for_period_prior,
                   t1.rank as rank_actual,
                   t2.rank2 as rank_prior
            from tab1 as t1
            inner join tab2 as t2 on t1.game_name = t2.game_name2            
        ) as t3
        order by increment_days_in_top DESC, ROUND(increment_percent_avg_players::numeric, 2)::float DESC, rank_actual
        """

        actual_data = self.pg_hook.get_records(query_actual_data)
        new_in_top, go_out_from_top = self.new_go_out(
            query_new_in_top=query_new_in_top,
            query_go_out_from_top=query_go_out_from_top,
            columns_new=["rank", "game_name", "top_for_period", "avg_for_period"],
            columns_go_out=[
                "rank2",
                "game_name2",
                "top_for_period2",
                "avg_for_period2",
            ],
        )
        stay_in_top = self.pg_hook.get_records(query_stay_in_top)
        cummulativ_total_for_year = self.pg_hook.get_records(
            self._cummulativ_total_for_year()
        )
        actual_data_full = self.pg_hook.get_records(query_actual_data_full)

        return {
            "actual_data": actual_data,
            "new_in_top": new_in_top,
            "go_out_from_top": go_out_from_top,
            "stay_in_top": stay_in_top,
            "cummulativ_total_for_year": cummulativ_total_for_year,
            "actual_data_full": actual_data_full,
        }

    def _format_num(self, num: float) -> str:
        integer_part, decimal_part = str(num).split(".")
        formatted_integer_part = "{:,}".format(int(integer_part)).replace(",", " ")
        formatted_number = formatted_integer_part + "," + decimal_part
        return formatted_number

    def execute(self, context: Context) -> None:
        self._check_args()
        self.pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        actual_date = self._find_actual_date(context)
        prior_date = self._find_prior_date(actual_date)

        path_save = os.path.join(self.path_save, self.date_period_type)
        pathlib.Path(path_save).mkdir(parents=True, exist_ok=True)

        path_save_data = os.path.join(
            path_save, f"{actual_date}_{self.date_period_type}.json"
        )

        columns_commulativ = ["rank", "game_name", "top_for_period", "avg_for_period"]

        if self.date_period_type == "day":
            data_dict = self._analysis_day(
                actual_date=actual_date, prior_date=prior_date
            )
            columns = ["rank", "game_name", "players_count"]
            columns_increment = [
                "increment_percent_players",
                "game_name",
                "players_count",
                "rank_actual",
                "rank_prior",
            ]

        else:
            data_dict = self._analysis_not_day(
                actual_date=actual_date,
                prior_date=prior_date,
                date_period_type=self.date_period_type,
            )
            columns = ["rank", "game_name", "top_for_period", "avg_for_period"]
            columns_increment = [
                "increment_days_in_top",
                "increment_percent_avg_players",
                "game_name",
                "top_for_period_actual",
                "top_for_period_prior",
                "avg_for_period_actual",
                "avg_for_period_prior",
                "rank_actual",
                "rank_prior",
            ]

        data_dict_for_json = {
            i[0]: {k: [] for k in i[1]}
            for i in [
                ("actual_data", columns),
                ("new_in_top", columns),
                ("go_out_from_top", columns),
                ("stay_in_top", columns_increment),
                ("cummulativ_total_for_year", columns_commulativ),
            ]
        }

        if self.date_period_type != "day":
            data_dict_for_json["actual_data_full"] = {
                k: [] for k in data_dict_for_json["actual_data"].keys()
            }

        print(data_dict_for_json)
        for data_type, columns in data_dict_for_json.items():
            print(data_type, columns)
            actual_columns = list(columns.keys())

            for row in data_dict[data_type]:
                for value, key in zip(row, actual_columns):
                    if key in [
                        "players_count",
                        "avg_for_period",
                        "avg_for_period_now",
                        "avg_for_period_prior",
                        "avg_for_period_actual",
                    ]:
                        value = round(value / 1000, 1)
                        value = self._format_num(value)

                    if key in [
                        "increment_percent_avg_players",
                        "increment_percent_players",
                    ]:
                        value = round(value, 1)
                        value = self._format_num(value)
                    data_dict_for_json[data_type][key].append(value)
        print(data_dict_for_json)
        print("Сохранение")
        with open(path_save_data, "w") as f:
            json.dump(data_dict_for_json, f, ensure_ascii=False)
