from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from datetime import datetime, timedelta
import json

# 기본 설정 (Owner, 시작 날짜, 재시도 설정)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

# DAG 정의
dag = DAG(
    'news_dag',
    default_args=default_args,
    schedule_interval=timedelta(hours=2),  # 수정된 스케줄
    catchup=False  # 과거 데이터 재실행 안 함
)


# 언론사 리스트
news_sources = ["ytn", "sbs", "kbs", "yna"]

# Extract Lambda 호출 Task 리스트
extract_lambda_tasks = []

for source in news_sources:
    lambda_task = LambdaInvokeFunctionOperator(
        task_id=f'invoke_news_extract_{source}',
        function_name='news_extract_function',
        payload=json.dumps({
            "source": source,
            # 한국 시간(KST)으로 변환
            "start_time_str": "{{ (execution_date + macros.timedelta(hours=9) - macros.dateutil.relativedelta.relativedelta(hours=2)).strftime('%Y-%m-%dT%H:%M') }}",
            "end_time_str": "{{ (execution_date + macros.timedelta(hours=9)).strftime('%Y-%m-%dT%H:%M') }}"
        }),
        aws_conn_id=None, # MWAA에서는 필요 없음
        region_name='ap-northeast-2',
        execution_timeout=timedelta(minutes=5),  # 5분 제한
        dag=dag  # DAG 명시적으로 추가
    )
    extract_lambda_tasks.append(lambda_task)