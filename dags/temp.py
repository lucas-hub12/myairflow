from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
import pendulum
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import requests
from airflow.models import Variable

def generate_bash_commands(columns: list):
    cmds = []
    max_length = max(len(c) for c in columns)
    for c in columns:
        padding = " " * (max_length - len(c))  # 가변적인 공백 추가 (최대 길이에 맞춰 정렬)
        cmds.append(f'echo "{c}{padding} : ====> {{{{ {c} }}}}"')
    return "\n".join(cmds)

import requests
import os

def send_noti(msg):
    WEBHOOK_ID = os.getenv('DISCORD_WEBHOOK_ID')
    WEBHOOK_TOKEN = os.getenv('DISCORD_WEBHOOK_TOKEN')
    WEBHOOK_URL = f"https://discordapp.com/api/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}"
    data = { "content":msg }
    response = requests.post(WEBHOOK_URL, json=data)

    if response.status_code == 204:
        print("메시지가 성공적으로 전송되었습니다.")
    else:
        print(f"에러 발생: {response.status_code}, {response.text}")
        
def print_kwargs(**kwargs):
    print("kwargs====>", kwargs)
    for k, v in kwargs.items():
        print(f"{k} : {v}")
    msg = f"{kwargs['dag'].dag_display_name} {kwargs['task'].task_id} {kwargs['data_interval_start'].in_tz('Asia/Seoul').strftime('%Y%m%d%H')} OK/LUCAS"
    from myairflow.notify import send_noti
    send_noti(msg)

local_tz = pendulum.timezone("Asia/Seoul")

# DAG 정의
with DAG(
    "temp",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 3, 11, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    send_notification = PythonOperator(
        task_id="send_notification",
        python_callable=print_kwargs
    )

    columns_b1 = [
        "data_interval_start", "data_interval_end", "logical_date", "ds", "ds_nodash",
        "ts", "ts_nodash_with_tz", "ts_nodash", "prev_data_interval_start_success",
        "prev_data_interval_end_success", "prev_start_date_success", "prev_end_date_success",
        "dag", "task", "task_instance", "run_id"
    ]
    cmds_b1 = generate_bash_commands(columns_b1)

    b1 = BashOperator(
        task_id="b_1", 
        bash_command=f"""
            echo "date ====================> `date`"
            {cmds_b1}
        """)

    cmds_b2_1 = [
        "execution_date", "next_execution_date", "next_ds", "next_ds_nodash",
        "prev_execution_date", "prev_ds", "prev_ds_nodash",
        "yesterday_ds", "tomorrow_ds", "prev_execution_date_success"
    ]
    cmds_b2_1 = generate_bash_commands(cmds_b2_1)

    b2_1 = BashOperator(task_id="b_2_1", bash_command=f"{cmds_b2_1}")
    
    b2_2 = BashOperator(task_id="b_2_2", bash_command="""echo "data_interval_start : {{ data_interval_start.in_tz('Asia/Seoul') }}" """)

    mkdir = BashOperator(
        task_id="mkdir",
        bash_command="""
            echo "data_interval_start : {{data_interval_start.in_tz('Asia/Seoul').strftime('%Y%m%d%H')}}",
            mkdir -p ~/data/seoul/{{data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H')}}
        """)
    # # send_notification = EmptyOperator(
    #     task_id="send_notification",
    #     python_callable=send_discord_alert,
    #      trigger_rule=TriggerRule.ONE_FAILED)
    

    # end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_SUCCESS)

    start >> b1
    b1 >> [b2_1, b2_2]
    [b2_1, b2_2] >> mkdir
    mkdir >> end
    mkdir >> send_notification

if __name__ == "__main__":
    dag.test()