from airflow import DAG
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator as BaseLambdaInvokeFunctionOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import ast
import hashlib
from botocore.config import Config

# ✅ Lambda 실행 시 타임아웃 문제 해결을 위한 커스텀 오퍼레이터
class LambdaInvokeFunctionOperator(BaseLambdaInvokeFunctionOperator):
    """
    Custom Lambda Operator to extend default timeout settings for boto3 connections to AWS.
    This prevents the default 60-second timeout issue when invoking a Lambda function synchronously.
    """

    def __init__(self, *args, **kwargs):
        config_dict = {
            "connect_timeout": 900,  # ✅ 15분 동안 AWS 연결 유지
            "read_timeout": 900,  # ✅ 15분 동안 응답을 기다릴 수 있도록 설정
            "tcp_keepalive": True,
        }
        self.config = Config(**config_dict)

        super().__init__(*args, **kwargs)

    def execute(self, context):
        hook = LambdaHook(aws_conn_id=self.aws_conn_id, config=self.config)
        self.hook = hook  # ✅ Airflow가 Lambda 실행할 때 이 Hook을 사용
        return super().execute(context)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

dag = DAG(
    'community_dag',
    default_args=default_args,
    schedule_interval=None,  # 수동 실행
    catchup=False
)

# 넘겨받은 데이터 확인하는 Python 함수
def process_issue_list(**kwargs):
    issue_list = kwargs['dag_run'].conf.get('issue_list', [])

    # issue_list가 문자열이면 JSON 리스트로 변환
    if isinstance(issue_list, str):
        issue_list = ast.literal_eval(issue_list)

    unique_car_models = list(set(item['car_model'] for item in issue_list))  # 중복 제거
    # Variable에 저장 (덮어쓰기 가능성 있음)
    Variable.set("unique_car_models", json.dumps(unique_car_models, ensure_ascii=False))
    data_interval_start = kwargs['dag_run'].conf.get('data_interval_start', None)
    data_interval_end = kwargs['dag_run'].conf.get('data_interval_end', None)

    print("Received issue list:", issue_list)
    print("Execution time window:", data_interval_start, "to", data_interval_end)
    print("Unique car models:", unique_car_models)

    # 커뮤니티 데이터 처리 로직 추가 가능
    # 1. issue_list 기반으로 커뮤니티 사이트 크롤링
    # 2. sentiment 분석 및 지표 계산
    # 3. 결과를 RDS에 업데이트

process_issue_task = PythonOperator(
    task_id="process_issue_list",
    python_callable=process_issue_list,
    provide_context=True,
    dag=dag
)

# 커뮤니티 리스트
# communities = ["dcinside", "bobaedream"]
communities = ["dcinside"]  # test용

# Extract Lambda 호출 Task 리스트
extract_lambda_tasks = []

# ✅ Variable.get()을 사용하여 unique_car_models 가져오기
try:
    unique_car_models = json.loads(Variable.get("unique_car_models"))
except:
    unique_car_models = []  # Variable이 아직 설정되지 않았다면 빈 리스트

for community in communities:
    for car_model in unique_car_models:
        # ✅ 한글 Task ID 방지 (hash 처리)
        hashed_model = hashlib.md5(car_model.encode()).hexdigest()[:6]  # ASCII 문자 유지

        lambda_task = LambdaInvokeFunctionOperator(
            task_id=f'crawl_{community}_{hashed_model}',
            function_name='bobae-crawler',
            payload=json.dumps({
                "community": community,
                "keyword": car_model,
                # "start_time_str": "{{ (data_interval_start + macros.timedelta(hours=9)).strftime('%Y-%m-%dT%H:%M') }}",
                # "end_time_str": "{{ (data_interval_end + macros.timedelta(hours=9)).strftime('%Y-%m-%dT%H:%M') }}"
                "start_time_str": "2024-07-02T00:00", # test
                "end_time_str": "2024-07-02T06:00" # test
            }),
            aws_conn_id=None,
            region_name='ap-northeast-2',
            execution_timeout=timedelta(minutes=15),
            dag=dag
        )
        extract_lambda_tasks.append(lambda_task)


# DAG 실행 순서 설정
process_issue_task >> extract_lambda_tasks