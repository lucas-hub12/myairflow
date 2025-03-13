from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonVirtualenvOperator
import pendulum
import sys
import os

sys.path.append('/home/lucas/.pyenv/versions/3.12.3/lib/python3.12/site-packages')
# myetl 패키지에서 함수 가져오기
from myairflow.func import load_data_pq
from myairflow.func import save_agg_csv

# DAG 정의
with DAG(
    "myetl",
    schedule="@hourly",
    start_date=pendulum.datetime(2024, 3, 12, tz="Asia/Seoul"),
    catchup=False,  # ✅ 과거 실행 방지
    max_active_runs=1,
) as dag:
    
    start = EmptyOperator(task_id="start")

    make_data = BashOperator(
        task_id="make_data",
        bash_command="""
        /home/lucas/airflow/make_data.sh /home/lucas/data/{{data_interval_start.in_tz('Asia/Seoul').strftime('%Y%m%d%H')}}
        """,
    )

    load_data = PythonVirtualenvOperator(
        task_id="load_data",
        python_callable=load_data_pq,  # ✅ 함수명 직접 전달
        requirements=["pandas", "pyarrow"],  # ✅ "pands" → "pandas" 수정
        system_site_packages=True,  # ✅ Airflow 패키지 사용 가능하도록 설정
        op_kwargs={"dis_path": "{{data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H')}}"}
    )

    agg_data = PythonVirtualenvOperator(
        task_id="agg_data",
        python_callable=save_agg_csv,  # ✅ 함수명 직접 전달
        requirements=["pandas", "pyarrow"],  # ✅ "pands" → "pandas" 수정
        system_site_packages=True,  # ✅ Airflow 패키지 사용 가능하도록 설정
        op_kwargs={"dis_path": "{{data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H')}}"}
    )

    end = EmptyOperator(task_id="end")

# DAG 실행 순서
start >> make_data >> load_data >> agg_data >> end