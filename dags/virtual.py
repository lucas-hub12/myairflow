from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator, PythonOperator 
import pendulum

# DAG 정의
with DAG(
    "virtual",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 3, 12, tz="Asia/Seoul"),
    default_args={
        "depends_on_past": False,
    },
    max_active_runs=1,
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    def f_python(**kwargs):
        from myairflow.send_notify import send_noti
        ti = kwargs['data_interval_start'].in_tz('Asia/Seoul').format('YYYYMMDDHH')
        send_noti(f"time {ti}: LUCAS python")

    def f_vpython(dis):
        from myairflow.send_notify import send_noti
        ti = dis.in_tz('Asia/Seoul').format('YYYYMMDDHH')
        send_noti(f"time: {ti} : LUCAS vpython")

    t_vpython = PythonVirtualenvOperator(
        task_id="t_vpyhon",
        python_callable=f_vpython,
        requirements=[
            "git+https://github.com/lucas-hub12/myairflow.git@0.1.0"
        ],
        op_args=["{{ data_interval_start'].in_tz('Asia/Seoul').format('YYYYMMDDHH') }}"]
        )
    t_python = PythonOperator(task_id="t_pyhon", python_callable=f_python)

    start >> t_vpython >> t_python >> end

if __name__ == "__main__":
    dag.test()