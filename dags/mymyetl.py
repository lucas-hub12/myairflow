from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonVirtualenvOperator
import pendulum

with DAG (
    "myetl",
    schedule="@hourly",
    start_date=pendulum.datetime(2024, 3, 12, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
) as dag:
    
    start = EmptyOperator(task_id="start")
     
    make_data = BashOperator(
          task_id="make_data",
          bash_command=
          "bash /home/lucas/airflow/make_data.sh /home/lucas/data/{{ data_interval_start.in_tz('Asia/Seoul').strftime('%Y%m%d%H') }}"
          )
    def fn_load_data(dis):
          from myairflow.func import load_data_pq
          load_data_pq(dis)


    load_data = PythonVirtualenvOperator(
          task_id="load_data",
          python_callable=fn_load_data,  # ✅ 함수명 직접 전달
          requirements=["git+https://github.com/lucas-hub12/myairflow.git@0.1.4"],  # ✅ "pands" → "pandas" 수정
          op_args=["{{ data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H') }}"]
    )

    def fn_save_agg(dis):  
          from myairflow.func import save_agg_csv
          save_agg_csv(dis)
        
    agg_data = PythonVirtualenvOperator(
          task_id="agg_data",
          python_callable=fn_save_agg,  # ✅ 함수명 직접 전달
          requirements=["git+https://github.com/lucas-hub12/myairflow.git@0.1.4"],
          op_args=["{{ data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H') }}"]
    )

    end = EmptyOperator(task_id="end")

    # DAG 실행 순서
    start >> make_data >> load_data >> agg_data >> end