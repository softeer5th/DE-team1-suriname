from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

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
def process_community_data(**kwargs):
    issue_list = kwargs['dag_run'].conf.get('issue_list', [])
    data_interval_start = kwargs['dag_run'].conf.get('data_interval_start', None)
    data_interval_end = kwargs['dag_run'].conf.get('data_interval_end', None)

    print("Received issue list:", issue_list)
    print("Execution time window:", data_interval_start, "to", data_interval_end)

    # 커뮤니티 데이터 처리 로직 추가 가능
    # 1. issue_list 기반으로 커뮤니티 사이트 크롤링
    # 2. sentiment 분석 및 지표 계산
    # 3. 결과를 RDS에 업데이트

process_task = PythonOperator(
    task_id="process_community_data",
    python_callable=process_community_data,
    provide_context=True,
    dag=dag
)

process_task