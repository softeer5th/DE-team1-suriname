## Airflow
### news_dag.py
#### 작업 흐름
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

### 추가 설명
- 실행과정에서 오류가 발생 시 slack alert를 개발자에게 보내기
  - `default_args`에 `on_failure_callback`으로 `send_slack_alert_callback` 함수를 지정했습니다
  - DAG의 어떤 Task가 실패하면 자동으로 이 콜백 함수가 실행되어 slack에 알림을 전달하는 람다를 호출합니다
  - 실패한 Task의 정보(실행 일시, 로그 URL 등)를 Slack 채널의 webhook url을 통해 전달합니다
