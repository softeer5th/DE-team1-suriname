from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import base64

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
    # schedule_interval=timedelta(hours=24),  # 수정된 스케줄
    schedule_interval=None,  # test
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
            # "start_time_str": "{{ (data_interval_start + macros.timedelta(hours=9)).strftime('%Y-%m-%dT%H:%M') }}",
            # "end_time_str": "{{ (data_interval_end + macros.timedelta(hours=9)).strftime('%Y-%m-%dT%H:%M') }}"
            "start_time_str": "2024-07-02T00:00", # test
            "end_time_str": "2024-07-03T00:00" # test
        }),
        aws_conn_id=None, # MWAA에서는 필요 없음
        region_name='ap-northeast-2',
        execution_timeout=timedelta(minutes=5),  # 5분 제한
        dag=dag  # DAG 명시적으로 추가
    )
    extract_lambda_tasks.append(lambda_task)


s3_news_data = Variable.get("S3_NEWS_DATA", "s3a://aws-seoul-suriname/data/news/")
s3_news_output = Variable.get("S3_NEWS_OUTPUT", "s3a://aws-seoul-suriname/data/news/output/")
accident_keyword_original = Variable.get("ACCIDENT_KEYWORD")
encoded_value = base64.b64encode(json.dumps(accident_keyword_original, ensure_ascii=False).encode('utf-8')).decode('utf-8')
Variable.set("ACCIDENT_KEYWORD_ENCODED", encoded_value)
accident_keyword = Variable.get("ACCIDENT_KEYWORD_ENCODED")
gpt = Variable.get("GPT")

entryPointArguments = [
    "--data_source", s3_news_data,
    "--output_uri", s3_news_output,
    # "--batch_period", "{{ (data_interval_start + macros.timedelta(hours=9)).strftime('%Y-%m-%d-%H-%M-00') }}_{{ (data_interval_end + macros.timedelta(hours=9)).strftime('%Y-%m-%d-%H-%M-00') }}",
    "--batch_period", "2024-07-02-00-00-00_2024-07-03-00-00-00", # test
    "--accident_keyword", accident_keyword,
    "--gpt", gpt
]

# **EMR Serverless 실행 Task**
emr_serverless_task = EmrServerlessStartJobOperator(
    task_id='run_news_emr_transform',
    application_id=Variable.get("EMR_APPLICATION_ID"),  # MWAA Variable에서 가져옴
    execution_role_arn="arn:aws:iam::572660899671:role/service-role/AmazonEMR-ExecutionRole-1739724269830",  # EMR 실행 역할
    job_driver={
        "sparkSubmit": {
            "entryPoint": Variable.get("ENTRY_POINT"),  # S3에 저장된 Spark 실행 코드
            "entryPointArguments": entryPointArguments,
            "sparkSubmitParameters": "--conf spark.executor.memory=4g --conf spark.driver.memory=2g"
        }
    },
    configuration_overrides={},
    aws_conn_id=None,
    dag=dag
)

# Lambda 실행 이후 EMR Serverless 실행
extract_lambda_tasks >> emr_serverless_task

