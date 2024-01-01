import os
import pathlib

import pendulum
from airflow.models import BaseOperator
from airflow import AirflowException
from airflow.utils.context import Context
from airflow.providers.postgres.hooks.postgres import PostgresHook


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

    def _analysis_day(
        self,
        actual_date: str,
        prior_date: str,
    ) -> dict:
        # запрос текущего топа
        query_actual_data = f"""
        select rank,
        game_name,
        players_count, 
        date
        from steam.steam_data
        where date = '{actual_date}'::date
        """

        subqueries = f"""
        with tab1 as (
        select *
        from steam.steam_data
        where date = '{actual_date}'::date  
        )
        
        with tab2 as (
        select *
        from steam.steam_data
        where date = '{prior_date}'::date  
        )
        """

        # запрос игр, которые вошли в топ
        query_new_in_top = f"""
        {subqueries}
        
        select t1.rank, t1.game_name, t1.players_count
        from tab1 as t1
        full join tab2 t2 on t1.game_name = t2.game_name
        where t2.game_name is null
        order by t1.rank
        """

        # запрос игр, которые из топа вышли
        query_go_out_from_top = f"""
        {subqueries}
        
        select t2.rank, t2.game_name, t2.players_count
        from tab1 as t1
        full join tab2 t2 on t1.game_name = t2.game_name
        where t1.game_name is null
        order by t2.rank
        """

        query_stay_in_top = f"""
        {subqueries}
        
        select 
        increment_percent, 
        game_name, 
        players_count,
        rank_actual,
        rank_prior
        from (
            select t1.game_name as game_name,
                   t1.rank as rank_actual, 
                   t2.rank as rank_prior,
                   (t1.players_count - t2.players_count)::float/t2.players_count * 100 as increment_percent,
                   t1.players_count as players_count  
            from tab1 as t1
            inner join tab2 as t2 on t1.page_name = t2.page_name            
        ) as t3
        order by ROUND(increment_percent::numeric, 2)::float DESC
        """

        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        actual_data = pg_hook.get_records(query_actual_data)
        new_in_top = pg_hook.get_records(query_new_in_top)
        go_out_from_top = pg_hook.get_records(query_go_out_from_top)
        stay_in_top = pg_hook.get_records(query_stay_in_top)

        return {
            "actual_data": actual_data,
            "new_in_top": new_in_top,
            "go_out_from_top": go_out_from_top,
            "stay_in_top": stay_in_top,
        }

    def _analysis_not_day(
        self, actual_date: str, prior_date: str, date_period_type: str
    ) -> dict:
        # запрос текущего топа
        query_actual_data = f"""
        select row_number() OVER(order by top_for_period DESC, avg_for_period desc) as rank,
        game_name,
        top_for_period, 
        avg_for_period
        from (select game_name, 
                     COUNT(*) as top_for_period,
                     AVG(players_count) as avg_for_period
              from (select * 
                    from steam.steam_data 
                    where date_trunc('{date_period_type}', date) = '{actual_date}') as tab1
              group by game_name
              ) as tab2
        """

        subqueries = f"""
        with tab1 as (
        select row_number() OVER(order by top_for_period DESC, avg_for_period desc) as rank,
        game_name,
        top_for_period, 
        avg_for_period
        from (select game_name, 
                     COUNT(*) as top_for_period,
                     AVG(players_count) as avg_for_period
              from (select * 
                    from steam.steam_data 
                    where date_trunc('{date_period_type}', date) = '{actual_date}') as tab1
              group by game_name
              ) as tab2
        )
        
        with tab2 as (
        select row_number() OVER(order by top_for_period DESC, avg_for_period desc) as rank,
        game_name,
        top_for_period, 
        avg_for_period
        from (select game_name, 
                     COUNT(*) as top_for_period,
                     AVG(players_count) as avg_for_period
              from (select * 
                    from steam.steam_data 
                    where date_trunc('{date_period_type}', date) = '{prior_date}') as tab1
              group by game_name
              ) as tab2
        )
        """

        # запрос игр, которые вошли в текущем периоде
        query_new_in_top = f"""
        {subqueries}
        
        select t1.rank, t1.game_name, t1.top_for_period, t1.avg_for_period
        from tab1 as t1
        full join tab2 t2 on t1.game_name = t2.game_name
        where t2.game_name is null
        order by t1.rank
        """

        # запрос игр, которые вышли из топа в текущем периоде
        query_new_in_top = f"""
        {subqueries}
        
        select t2.rank, t2.game_name, t2.top_for_period, t2.avg_for_period
        from tab1 as t1
        full join tab2 t2 on t1.game_name = t2.game_name
        where t1.game_name is null
        order by t2.rank
        """

        query_stay_in_top = f"""
        {subqueries}
        
        select 
        increment_days_in_top,
        increment_percent_avg_players, 
        game_name, 
        top_for_period_actual,
        top_for_period_prior,
        avg_for_period_now,
        avg_for_period_prior,
        rank_actual,
        rank_prior
        from (
            select 
                   (t1.top_for_period - t2.top_for_period)::int as increment_days_in_top,
                   (t1.avg_for_period - t2.avg_for_period)::float/t2.avg_for_period * 100 as increment_percent_avg_players,
                   t1.game_name as game_name,
                   t1.top_for_period as top_for_period_actual, 
                   t2.top_for_period as top_for_period_prior,
                   t1.avg_for_period as avg_for_period_now,
                   t2.avg_for_period as avg_for_period_prior,
                   t1.rank as rank_actual, 
                   t2.rank as rank_prior
            from tab1 as t1
            inner join tab2 as t2 on t1.page_name = t2.page_name            
        ) as t3
        order by increment_days_in_top DESC, ROUND(increment_percent::numeric, 2)::float DESC
        """

    def execute(self, context: Context) -> None:
        self._check_args()
        actual_date = self._find_actual_date(context)
        prior_date = self._find_prior_date(actual_date)

        path_save = os.path.join(self.path_save, self.date_period_type)
        pathlib.Path(path_save).mkdir(parents=True, exist_ok=True)

        path_save_data = os.path.join(
            path_save, f"{actual_date}_{self.date_period_type}.json"
        )

        query_actual_data = f"""
        select rank,
        game_name,
        players_count, 
        date
        
        """
