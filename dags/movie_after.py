from pprint import pprint
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    PythonVirtualenvOperator,
)

from airflow.sensors.filesystem import FileSensor

DAG_ID = "movie_after"

with DAG(
    DAG_ID,
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=5,
    description="movie after",
    schedule="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 5),
    catchup=True,
    tags=["api", "movie", "sensor"],
) as dag:
    REQUIREMENTS = ["git+https://github.com/ppabam/movie.git@0.4.0"]
    BASE_DIR = f"~/data/{DAG_ID}"
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    check_done = FileSensor(
        task_id="check.done",
        filepath="/home/tom/data/movies/done/dailyboxoffice/{{ds_nodash}}/_DONE",
        fs_conn_id="fs_after_movie",
        poke_interval=180,  # 3분마다 체크
        timeout=3600,  # 1시간 후 타임아웃
        mode="reschedule",  # 리소스를 점유하지 않고 절약하는 방식
    )

    def fn_gen_meta(**kwargs):
        import json
        print(json.dumps(kwargs, indent=4, ensure_ascii=False))
        # TODO f"{base_path}/meta/meta.parquet -> 경로로 저장


    gen_meta = PythonVirtualenvOperator(
        task_id="gen.meta",
        python_callable=fn_gen_meta,
        requirements=REQUIREMENTS,
        system_site_packages=False,
    )

    def fn_gen_movie(base_path, ds, **kwargs):
        import json
        print(json.dumps(kwargs, indent=4, ensure_ascii=False))
        print(f"base_path: {base_path}")
        # TODO -> f"{base_path}/dailyboxoffice/ 생성
        # TODO movie airflow 의 merge.data 와 같은 동작 ( meta.parquet 사용 )
        # TODO 파티션은 dt, multiMovieYn, repNationCd

    gen_movie = PythonVirtualenvOperator(
        task_id="gen.movie",
        python_callable=fn_gen_movie,
        requirements=REQUIREMENTS,
        system_site_packages=False,
        op_kwargs={"base_path": BASE_DIR},
    )

    make_done = BashOperator(
        task_id="make.done",
        bash_command="""
        DONE_BASE=$BASE_DIR/done
        echo $DONE_BASE
        mkdir -p $DONE_BASE/{{ ds_nodash }}
        touch $DONE_BASE/{{ ds_nodash }}/_DONE
        """,
        env={'BASE_DIR':BASE_DIR},
        append_env = True
    )

    start >> check_done >> gen_meta >> gen_movie >> make_done >> end
