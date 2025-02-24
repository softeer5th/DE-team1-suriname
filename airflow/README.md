## Airflow
### news_dag.py
#### 작업 흐름
<div style="text-align: center;">
    <img style="width: 80%; height: auto;" alt="Image" src="https://github.com/user-attachments/assets/e859c953-77dc-4fbd-9cae-fe6194719b1c" />
</div>   

1. **뉴스 Extract task**
    - 언론사 리스트(“ytn”, “sbs”, “kbs”, “yna”)에 대해 각각 병렬로 Operator를 생성합니다.
    - `news_extract_function`를 각각 호출해 배치기간동안의 크롤링을 수행합니다.

2. **EmrServerless task (뉴스 데이터 변환)**
    - 모든 뉴스 추출 Lambda Task가 완료된 뒤 실행됩니다.
    - EmrServerlessStartJobOperator을 통해 Spark Job을 제출합니다.
    - 이때, S3에 저장된 Spark 코드를 엔트리포인트로 사용하고, 필요한 매개변수를 Variables에서 가져와 전달합니다
    - 한글이 포함된 리스트, dict의 경우 encoding을 수행해서 전달합니다

3. **RDS Update task**
    - `lambda_load_news`를 호출합니다.
    - S3에 저장된 EMR 작업 결과(뉴스 데이터)를 RDS 테이블에 병합/업데이트하고, 이슈 존재여부에 따라 is_issue 값을 설정

4. **이슈 여부 확인 후 분기 task**
    - RDS에서 `is_issue = TRUE`인 데이터가 존재하는지 조회합니다.
    - 조회 결과에 따라 “trigger_community_dag” 혹은 “skip_community_dag”로 분기되게 됩니다.
        - is_issue가 있으면 → “trigger_community_dag”(커뮤니티 ETL 수행)
        - is_issue가 없으면 → “skip_community_dag”(배치 종료)

5. **커뮤니티 DAG 트리거 task, 커뮤니티 DAG 스킵 task**
    - If 분기 결과 “trigger_community_dag” → 커뮤니티 관련 DAG(“community_dag”)를 실행
    - Else “skip_community_dag” → 아무 작업 없이 다음 단계 스킵

### community_dag.py
#### 작업 흐름
<div style="text-align: center;">
    <img style="width: 80%; height: auto;" alt="Image" src="https://github.com/user-attachments/assets/0555b75b-6f51-4aeb-b6b3-4743b948a7d4" />
</div>   

1. **이슈 리스트 처리 Task**
    - DAG 실행 시 전달받은 `issue_list`를 파싱하고, 필요한 변수를 Airflow Variable에 저장합니다.

2. **커뮤니티 Extract Task**
    - “dcinside”, “femco”와 같이 미리 정의된 커뮤니티별로 람다 함수를 병렬 호출하여, 필요한 게시글을 크롤링 또는 수집하는 작업을 수행합니다.
    - 각 커뮤니티별 Operator를 생성하여 동시에 크롤링을 진행합니다.

3. **EmrServerless Task (커뮤니티 데이터 변환)**
    - 모든 이슈에 대해 Extract Task가 완료된 뒤, Spark 코드를 EMR Serverless 작업으로 제출합니다.
    - 이때 S3에 저장된 Spark 코드를 엔트리포인트로 사용하고, 필요한 실행 매개변수들을 Airflow Variable에서 불러와 Spark에 전달합니다(예: 배치 기간, 키워드 목록, GPT 관련 문자열, 이슈 리스트 등).

4. **Redshift Task**
    - Lambda 함수를 호출하여, EMR Serverless 결과데이터와 RDS 테이블의 정보를 활용해 Redshift에 추가합니다.

5. **Slack 알림 Task**
    - 모든 작업이 완료된 후, Slack 및 메시지 정보를 담은 페이로드를 Lambda로 전달하여, Slack으로 알림을 보냅니다.
      - 발생 이슈에 대해 이슈 단계, 대시보드 링크, 최초 발생시간 정보를 전달합니다.
    - 실패 시에도 콜백 함수가 연동되도록 설정하여 작업 상태를 모니터링할 수 있게끔 구성합니다.

### 추가 설명
- 실행과정에서 오류가 발생 시 slack alert를 개발자에게 보내기
  - `default_args`에 `on_failure_callback`으로 `send_slack_alert_callback` 함수를 지정했습니다
  - DAG의 어떤 Task가 실패하면 자동으로 이 콜백 함수가 실행되어 slack에 알림을 전달하는 람다를 호출합니다
  - 실패한 Task의 정보(실행 일시, 로그 URL 등)를 Slack 채널의 webhook url을 통해 전달합니다
  - 이를 통해 문제를 빠르게 파악할 수 있게해 파이프라인 자체의 견고함을 높이고자 했습니다.
- Airflow Timeout
  - 커뮤니티 크롤링의 경우 동적으로 크롤링을 수행하기때문에 Airflow가 기본적으로 설정한 시간내에 크롤링을 완료하지 못했습니다.
  - 때문에 아직 크롤링이 진행 중임에도 실패했다고 판단해 재시도하는 문제가 발생했습니다.
  - 이를 해결하기 위해 custom operator를 생성해 timout시간을 늘려 해결했습니다.