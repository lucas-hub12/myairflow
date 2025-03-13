from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonVirtualenvOperator
import pendulum
import pandas as pd
import os

CSV_PATH = "/home/lucas/data/data.csv"
PARQUET_PATH = "/home/lucas/data/data.parquet"
AGG_CSV_PATH = "/home/lucas/data/agg.csv"

def load_data_pq(b):
    if os.path.exists(CSV_PATH):
        df = pd.read_csv(CSV_PATH)
        df.to_parquet(PARQUET_PATH, engine="pyarrow")
        print(f"✅ CSV → Parquet 변환 완료: {PARQUET_PATH}")
        return True
    else:
        print(f"❌ 오류: CSV 파일을 찾을 수 없습니다! ({CSV_PATH})")

def save_agg_csv(a):
    if os.path.exists(PARQUET_PATH):
        df = pd.read_parquet(PARQUET_PATH)

        # Group By(name) + count(value)
        agg_df = df.groupby("name")["value"].count().reset_index()
        agg_df.rename(columns={"value": "count"}, inplace=True)

        # CSV 저장
        agg_df.to_csv(AGG_CSV_PATH, index=False)
        print(f"Parquet Group By → CSV 저장 완료: {AGG_CSV_PATH}")
    else:
        print(f"오류: Parquet 파일을 찾을 수 없습니다! ({PARQUET_PATH})")


# DAG 정의
with DAG(
    "myetl",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 3, 12, tz="Asia/Seoul"),
    default_args={
        "depends_on_past": False,
    },
    max_active_runs=1,
) as dag:
    
    start = EmptyOperator(task_id="start")

    make_data = BashOperator(task_id="make_data",
                             bash_command="""
                             /home/lucas/airflow/make_data.sh /home/lucas/data/{{data_interval_start.in_tz('Asia/Seoul').strftime('%Y%m%d%H')}}",)
                              """)
    load_data = PythonVirtualenvOperator(task_id="load_data",
                                         python_callable= load_data_pq,
                                         requirements=["pands","pyarrow"],
                                         op_kwargs={"dis_path":"{{data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H')}}"}
                                         )
    
    agg_data = PythonVirtualenvOperator(task_id="agg_data",
                                        python_callable= save_agg_csv,
                                         requirements=["pands","pyarrow"],
                                         op_kwargs={"dis_path":"{{data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H')}}"}
                                         )
    end = EmptyOperator(task_id="end")

start >> make_data >> load_data >> agg_data >> end