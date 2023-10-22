import os
import datetime as dt

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

from airflow.decorators import task_group


from constants import PATH_WORK_FILES, DOMAIN_CONFIG, SEND_MESSAGES

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
    dag_id="test_send_message",
    tags=[
        "test",
        "wikipedia_views",
        "send_message",
    ],
    default_args=default_args,
    start_date=pendulum.datetime(2020, 1, 1).add(days=-1),
    schedule_interval="@hourly",
    template_searchpath=PATH_WORK_FILES,
)

start_oper = EmptyOperator(task_id="start_oper", dag=dag)
start_oper_2 = EmptyOperator(task_id="start_oper_2", dag=dag)

groups_message = []
for code in SEND_MESSAGES:

    @task_group(dag=dag, group_id=f"send_message_group_{code}")
    def send_message_group():
        start_oper_3 = EmptyOperator(task_id="start_oper_3", dag=dag)
        group_config = DOMAIN_CONFIG[code]
        for domain_code, sub_group_config in DOMAIN_CONFIG.items():
            sub_groups_message = []

            @task_group(dag=dag, group_id=f"send_message_subgroup_{domain_code}")
            def send_message_subgroup():
                not_end_day_week_month_year = EmptyOperator(
                    task_id="not_end_day_week_month_year", dag=dag
                )
                end_day = EmptyOperator(task_id="end_day", dag=dag)
                end_week = EmptyOperator(
                    task_id="end_week", dag=dag, trigger_rule="none_failed"
                )
                end_month = EmptyOperator(
                    task_id="end_month", dag=dag, trigger_rule="none_failed"
                )
                end_year = EmptyOperator(
                    task_id="end_year", dag=dag, trigger_rule="none_failed"
                )

                def _check_end_week_month_year(time_correction: dict, **context):
                    list_branches = []

                    data_interval_start = context["data_interval_start"].add(
                        hours=time_correction
                    )
                    if data_interval_start.hour == 23:
                        list_branches.append(end_day.task_id)

                    if data_interval_start.day_of_week == 0:
                        list_branches.append(end_week.task_id)

                    if data_interval_start.day == data_interval_start.days_in_month:
                        list_branches.append(end_month.task_id)

                    if data_interval_start.year != data_interval_start.add(days=1).year:
                        list_branches.append(end_year.task_id)

                    if list_branches:
                        return list_branches
                    else:
                        return not_end_day_week_month_year.task_id

                check_end_week_month_year = BranchPythonOperator(
                    task_id="check_end_week_month_year",
                    python_callable=_check_end_week_month_year,
                    op_args=[sub_group_config["time_correction"]],
                )

                check_end_week_month_year >> [
                    end_day,
                    end_week,
                    end_month,
                    end_year,
                    not_end_day_week_month_year,
                ]
                end_day >> end_week >> end_month >> end_year

            sub_groups_message.append(send_message_subgroup())
        else:
            text = ""
            for index, _ in enumerate(sub_groups_message):
                text += f"sub_groups_message[{index}] >> "
            text = text.removesuffix(" >> ")

            eval(text)

    groups_message.append(send_message_group())


start_oper >> start_oper_2 >> groups_message


# group = []
# for index in range(10):

#     @task_group(dag=dag, group_id=f"test_{index}")
#     def send_message_group():
#         not_end_day = EmptyOperator(task_id="not_end_day", dag=dag)
#         not_end_day

#     group.append(send_message_group())


# text = ""
# for index, _ in enumerate(group):
#     text += f"group[{index}] >> "
# text = text.removesuffix(" >> ")


# start_oper >> start_oper_2 >> eval(text)
