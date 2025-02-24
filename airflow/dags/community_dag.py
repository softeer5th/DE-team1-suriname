from airflow import DAG
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator as BaseLambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
# from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import ast
import hashlib
from botocore.config import Config
import json
import base64

test_start_time = Variable.get("TEST_START_TIME")
test_end_time = Variable.get("TEST_END_TIME")

def send_slack_alert_callback(context):

    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url

    slack_data = {
        "webhook_url" : Variable.get("DEV_WEBHOOK_URL"),
        "payload" : {
            "dag_id": dag_id,
            "task_id" : task_id,
            "execution_date": execution_date,
            "log_url" : log_url,
        }
    }

    json_payload = json.dumps(slack_data, default=str)

    
    # LambdaInvokeFunctionOperator 인스턴스를 생성하고 실행
    operator = LambdaInvokeFunctionOperator(
        task_id='send_slack_alert_for_developer',
        function_name='lambda_slack_alert_for_developer',
        payload=json_payload,
        aws_conn_id=None,
        region_name='ap-northeast-2',
        execution_timeout=timedelta(minutes=5)
    )
    operator.execute(context=context)

# ✅ "YYYY-MM-DDTHH:MM" -> "YYYY-MM-DD-HH-MM-SS" 변환 함수
def format_time_variable(time_str):
    return time_str.replace("T", "-").replace(":", "-") + "-00"

# ✅ 변환된 값
formatted_start_time = format_time_variable(test_start_time)
formatted_end_time = format_time_variable(test_end_time)

# ✅ test_batch_period 만들기
test_batch_period = f"{formatted_start_time}_{formatted_end_time}"

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
    'retries': 1,
    'on_failure_callback': send_slack_alert_callback 
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

    Variable.set("issue_list", issue_list)

    unique_car_models = list(set(item['car_model'] for item in issue_list))  # 중복 제거
    # unique_car_models = ['그랜저', '아반떼', '쏘나타'] # test
    # Variable에 저장 (덮어쓰기 가능성 있음)
    Variable.set("unique_car_models", json.dumps(unique_car_models, ensure_ascii=False))
    data_interval_start = kwargs['dag_run'].conf.get('data_interval_start', None)
    data_interval_end = kwargs['dag_run'].conf.get('data_interval_end', None)
    Variable.set("data_interval_start", data_interval_start)
    Variable.set("data_interval_end", data_interval_end)

    print("Received issue list:", issue_list)
    print("Execution time window:", data_interval_start, "to", data_interval_end)
    print("Unique car models:", unique_car_models)


process_issue_task = PythonOperator(
    task_id="process_issue_list",
    python_callable=process_issue_list,
    provide_context=True,
    dag=dag
)

# 커뮤니티 리스트
# communities = ["dcinside", "bobaedream"]
communities = ["dcinside", "femco"]  # test용

# Extract Lambda 호출 Task 리스트
extract_lambda_tasks = []

# ✅ Variable.get()을 사용하여 unique_car_models 가져오기
try:
    unique_car_models = json.loads(Variable.get("unique_car_models"))
except:
    unique_car_models = []  # Variable이 아직 설정되지 않았다면 빈 리스트

for community in communities:
    # # ✅ 한글 Task ID 방지 (hash 처리)
    # hashed_model = hashlib.md5(car_model.encode()).hexdigest()[:6]  # ASCII 문자 유지
    lambda_task = LambdaInvokeFunctionOperator(
        task_id=f'crawl_{community}',
        function_name='bobae-crawler',
        payload=json.dumps({
            "community": community,
            # "start_time_str": "{{ (data_interval_start + macros.timedelta(hours=9)).strftime('%Y-%m-%dT%H:%M') }}",
            # "end_time_str": "{{ (data_interval_end + macros.timedelta(hours=9)).strftime('%Y-%m-%dT%H:%M') }}"
            "start_time_str": test_start_time, # test
            "end_time_str": test_end_time # test
        }),
        aws_conn_id=None,
        region_name='ap-northeast-2',
        execution_timeout=timedelta(minutes=15),
        dag=dag
    )
    extract_lambda_tasks.append(lambda_task)

s3_community_data = Variable.get("S3_COMMUNITY_DATA", "s3a://aws-seoul-suriname/data/community/")
s3_community_output = Variable.get("S3_COMMUNITY_OUTPUT", "s3a://aws-seoul-suriname/data/community/output/")
community_accident_keyword_original = Variable.get("COMMUNITY_ACCIDENT_KEYWORD")
encoded_value = base64.b64encode(json.dumps(community_accident_keyword_original, ensure_ascii=False).encode('utf-8')).decode('utf-8')
Variable.set("COMMUNITY_ACCIDENT_KEYWORD_ENCODED", encoded_value)
accident_keyword = Variable.get("COMMUNITY_ACCIDENT_KEYWORD_ENCODED")

gpt = Variable.get("GPT")
issue_list_original = Variable.get("issue_list")
# issue_list_original = Variable.get("test_issue_list") # test
issue_list_encoded_value = base64.b64encode(json.dumps(issue_list_original, ensure_ascii=False).encode('utf-8')).decode('utf-8')
Variable.set("ISSUE_LIST_ENCODED", issue_list_encoded_value)
issue_list = Variable.get("ISSUE_LIST_ENCODED")


entryPointArguments = [
    "--data_source", s3_community_data,
    "--output_uri", s3_community_output,
    # "--batch_period", "{{ (data_interval_start + macros.timedelta(hours=9)).strftime('%Y-%m-%d-%H-%M-00') }}_{{ (data_interval_end + macros.timedelta(hours=9)).strftime('%Y-%m-%d-%H-%M-00') }}",
    "--batch_period", test_batch_period, # test
    "--community_accident_keyword", accident_keyword,
    "--gpt", gpt,
    "--issue_list", issue_list
]

# **EMR Serverless 실행 Task**
emr_serverless_task = EmrServerlessStartJobOperator(
    task_id='run_community_emr_transform',
    application_id=Variable.get("EMR_APPLICATION_ID"),  # MWAA Variable에서 가져옴
    execution_role_arn="arn:aws:iam::572660899671:role/service-role/AmazonEMR-ExecutionRole-1739724269830",  # EMR 실행 역할
    job_driver={
        "sparkSubmit": {
            "entryPoint": Variable.get("COMMUNITY_ENTRY_POINT"),  # S3에 저장된 Spark 실행 코드
            "entryPointArguments": entryPointArguments,
            "sparkSubmitParameters": "--conf spark.executor.memory=4g --conf spark.driver.memory=2g"
        }
    },
    configuration_overrides={},
    aws_conn_id=None,
    dag=dag
)

# **RDS 병합 및 업데이트 + 이슈주의도 calculate + view table append Lambda 실행 Task 추가**
lambda_load_community_task = LambdaInvokeFunctionOperator(
    task_id='invoke_lambda_load_community',
    function_name='lambda_rds_update_news',
    payload=json.dumps({
        "batch_period": test_batch_period,  # 테스트용
        # "batch_period": "{{ (data_interval_start + macros.timedelta(hours=9)).strftime('%Y-%m-%d-%H-%M-00') }}_{{ (data_interval_end + macros.timedelta(hours=9)).strftime('%Y-%m-%d-%H-%M-00') }}",
        "dbname": Variable.get("RDS_DBNAME"),
        "user": Variable.get("RDS_USER"),
        "password": Variable.get("RDS_PASSWORD"),
        "url": Variable.get("RDS_HOST"),
        "port": Variable.get("RDS_PORT"),
        "bucket_name": Variable.get("S3_BUCKET_NAME"),  # S3 버킷 정보
        "redshift_db": Variable.get("REDSHIFT_DB"),
        "redshift_workgroup": Variable.get("REDSHIFT_WORKGROUP")
    }),
    aws_conn_id=None,
    region_name='ap-northeast-2',
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# Slack Alert Task
send_slack_alert = LambdaInvokeFunctionOperator(
    task_id='send_slack_alert',
    function_name='lambda_slack_alert',
    payload=json.dumps({
        "dbname": Variable.get("RDS_DBNAME"),
        "user": Variable.get("RDS_USER"),
        "password": Variable.get("RDS_PASSWORD"),
        "url": Variable.get("RDS_HOST"),
        "port": Variable.get("RDS_PORT"),
        "webhook_url": Variable.get("USER_WEBHOOK_URL")
    }),
    aws_conn_id=None,
    region_name='ap-northeast-2',
    execution_timeout=timedelta(minutes=5),
    dag=dag
)




# DAG 실행 순서 설정
# chain(
#     process_issue_task,
#     extract_lambda_tasks,
#     emr_serverless_task,
#     lambda_load_community_task,
#     send_slack_alert
# )

# process_issue_task >> extract_lambda_tasks >> emr_serverless_task >> lambda_load_community_task >> send_slack_alert

process_issue_task >> emr_serverless_task >> lambda_load_community_task >> send_slack_alert

# test
# emr_serverless_task >> lambda_load_community_task