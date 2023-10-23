import os
import datetime as dt

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.decorators import task_group

from wikiviews.send_messages import SendMessages

from constants import PATH_WORK_FILES, DOMAIN_CONFIG, SEND_MESSAGES, BOTS

path_save_script = os.path.join(
    PATH_WORK_FILES,
    "script_load_postgres",
)
path_dz_file = os.path.join(
    PATH_WORK_FILES,
    "pageviews.gz",
)
path_analysis_save = os.path.join(
    PATH_WORK_FILES,
    "analysis",
)


default_args = {
    "wait_for_downstream": True,
    "retries": 10,
    "retry_delay": dt.timedelta(seconds=10),
    "execution_timeout": dt.timedelta(minutes=60),
    "end_date": pendulum.datetime(2021, 1, 1),
}

dag = DAG(
    dag_id="send_message",
    tags=[
        "test",
        "wikipedia_views",
        "send_message",
    ],
    default_args=default_args,
    start_date=pendulum.datetime(2020, 1, 1).add(days=1),
    schedule_interval=None,
    template_searchpath=PATH_WORK_FILES,
    max_active_runs=16,
)

start_oper = EmptyOperator(task_id="start_oper", dag=dag)

groups_message = []
for code in SEND_MESSAGES:
    for index_leng, domain_code in enumerate(sorted(DOMAIN_CONFIG.keys())):
        bots_list = BOTS[code]
        group_config = DOMAIN_CONFIG[code]
        sub_group_config = DOMAIN_CONFIG[domain_code]

        @task_group(dag=dag, group_id=f"send_message_group_{code}_{domain_code}")
        def send_message_subgroup():
            not_end_day = EmptyOperator(task_id="not_end_day", dag=dag)
            not_end_week_month_year = EmptyOperator(
                task_id="not_end_week_month_year", dag=dag
            )
            end_day = SendMessages(
                task_id="day",
                group_config=group_config,
                sub_group_config=sub_group_config,
                date_period_type="day",
                path_save=path_analysis_save,
                telegram_token_bot=os.environ.get(
                    bots_list[index_leng % len(bots_list)]
                ),
                chat_ids=DOMAIN_CONFIG[code]["message_settings"]["group_tokens"],
            )

            end_week = SendMessages(
                task_id="week",
                group_config=group_config,
                sub_group_config=sub_group_config,
                date_period_type="week",
                path_save=path_analysis_save,
                telegram_token_bot=os.environ.get(
                    bots_list[index_leng % len(bots_list)]
                ),
                chat_ids=DOMAIN_CONFIG[code]["message_settings"]["group_tokens"],
            )

            end_month = SendMessages(
                task_id="month",
                group_config=group_config,
                sub_group_config=sub_group_config,
                date_period_type="month",
                path_save=path_analysis_save,
                telegram_token_bot=os.environ.get(
                    bots_list[index_leng % len(bots_list)]
                ),
                chat_ids=DOMAIN_CONFIG[code]["message_settings"]["group_tokens"],
            )

            end_year = SendMessages(
                task_id="year",
                group_config=group_config,
                sub_group_config=sub_group_config,
                date_period_type="year",
                path_save=path_analysis_save,
                telegram_token_bot=os.environ.get(
                    bots_list[index_leng % len(bots_list)]
                ),
                chat_ids=DOMAIN_CONFIG[code]["message_settings"]["group_tokens"],
            )

            def _check_end_day(time_correction: dict, **context):
                data_interval_start = context["data_interval_start"].add(
                    hours=time_correction
                )
                if data_interval_start.hour == 23:
                    return end_day.task_id
                else:
                    return not_end_day.task_id

            def _check_end_week_month_year(time_correction: dict, **context):
                list_branches = []

                data_interval_start = context["data_interval_start"].add(
                    hours=time_correction
                )
                if data_interval_start.day_of_week == 0:
                    list_branches.append(end_week.task_id)

                if data_interval_start.day == data_interval_start.days_in_month:
                    list_branches.append(end_month.task_id)

                if data_interval_start.year != data_interval_start.add(days=1).year:
                    list_branches.append(end_year.task_id)

                if list_branches:
                    return list_branches
                else:
                    return not_end_week_month_year.task_id

            check_end_week_month_year = BranchPythonOperator(
                task_id="check_end_week_month_year",
                python_callable=_check_end_week_month_year,
                op_args=[sub_group_config["time_correction"]],
            )
            check_end_day = BranchPythonOperator(
                task_id="check_end_day",
                python_callable=_check_end_day,
                op_args=[sub_group_config["time_correction"]],
                trigger_rule="none_failed",
            )

            end_week_non_failed = EmptyOperator(
                task_id="end_week_non_failed",
                dag=dag,
                trigger_rule="none_failed",
            )
            end_month_non_failed = EmptyOperator(
                task_id="end_month_non_failed",
                dag=dag,
                trigger_rule="none_failed",
            )

            (check_end_day >> [end_day, not_end_day])

            (
                end_day
                >> check_end_week_month_year
                >> [end_week, end_month, end_year, not_end_week_month_year]
            )

            (
                # end_day_non_failed
                end_week
                >> end_week_non_failed
                >> end_month
                >> end_month_non_failed
                >> end_year
            )

        groups_message.append(send_message_subgroup())

# text = "start_oper >> "
# for index_2 in range(len(DOMAIN_CONFIG)):
#     text += f"groups_message[{index_2}] >> "
# text = text.removesuffix(" >> ")

# text = "start_oper >> "
# for index_2 in range(len(DOMAIN_CONFIG), len(DOMAIN_CONFIG) * 2):
#     text += f"groups_message[{index_2}] >> "
# text_2 = text.removesuffix(" >> ")


(
    start_oper
    >> groups_message[0]
    >> groups_message[1]
    >> groups_message[2]
    >> groups_message[3]
    >> groups_message[4]
    >> groups_message[5]
    >> groups_message[6]
    >> groups_message[7]
    >> groups_message[8]
    >> groups_message[9]
    >> groups_message[10]
    >> groups_message[11]
    >> groups_message[12]
    >> groups_message[13]
    >> groups_message[14]
)

(
    start_oper
    >> groups_message[15]
    >> groups_message[16]
    >> groups_message[17]
    >> groups_message[18]
    >> groups_message[19]
    >> groups_message[20]
    >> groups_message[21]
    >> groups_message[22]
    >> groups_message[23]
    >> groups_message[24]
    >> groups_message[25]
    >> groups_message[26]
    >> groups_message[27]
    >> groups_message[28]
    >> groups_message[29]
)
