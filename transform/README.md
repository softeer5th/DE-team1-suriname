# Transform
각 코드를 로컬에서 실행해 보기 위해서는 ```conf.py``` 파일을 만들어 필요한 변수를 전달해 주어야 합니다.
## EMR Serverless
- 대규모 데이터 처리를 위한 클라우드 기반의 관리형 빅데이터 플랫폼
- Apache Spark, Apache Hadoop, Hive, HBase 등 다양한 도구 지원
- 자동 Scaling 가능
- 사용한 만큼만 지불
- 작업 완료 후 자동 종료 설정
### emr_transform_news.py
    배치기간 동안 차량모델이 언급된 기사에 대해 사고 관련 여부에 따라 필터링을 수행하고 사고의 심각성을 GPT API를 통해 분석합니다.
    최종적으로 각 차량별로 발생한 이슈들에 대해 점수를 매겨 S3에 parqeut 형식으로 저장합니다.
#### 요구사항
`conf.py`에 아래 변수들을 설정해주어야 합니다.
  - `S3_NEWS_DATA`: S3에 저장된 입력 데이터 경로
  - `S3_NEWS_OUTPUT`: S3에 저장할 결과 데이터 경로
  - `S3_NEWS_BATCH_PERIOD`: batch 기간(_YYYY-mm-dd-HH-MM_)
  - `ACCIDENT_KEYWORD`: 검출할 사고 딕셔너리
  - `GPT`: gpt(모델명), api_key 딕셔너리
#### 작업 흐름
<div style="text-align: center;">
    <img style="width: 80%; height: auto;" alt="Image" src="https://github.com/user-attachments/assets/12970a37-1c3a-4f30-8341-332243612118" />
</div>   

### emr_transform_community.py
    이슈가 발생했을 때 같은 배치 기간에 수집된 디시 인사이드, 에펨코리아 커뮤니티 게시글에 대해 
    사고에 관련된 게시글들을 필터링 하고 GPT API를 활용해 게시글이 현대자동차 그룹에 대해 부정적인 게시글인지 분석 후 평균 이슈 점수를 계산합니다.
    이슈별 조회수 기준 상위 5개의 게시글들을 뽑아 평균 이슈 점수와 함께 S3에 parqeut 형식으로 저장합니다.
#### 요구사항
`conf.py`에 아래 변수들을 설정해주어야 합니다.
  - `S3_COMMUNITY_DATA`: S3에 저장된 입력 데이터 경로
  - `S3_COMMUNITY_OUTPUT`: S3에 저장할 결과 데이터 경로
  - `S3_COMMUNITY_BATCH_PERIOD`: batch 기간(_YYYY-mm-dd-HH-MM_)
  - `ACCIDENT_KEYWORD`: 검출할 사고 딕셔너리
  - `GPT`: gpt(모델명), api_key 딕셔너리
  - `ISSUE_LIST`: 이슈라고 선정된 딕셔너리
#### 작업 흐름
<div style="text-align: center;">
    <img style="width: 80%; height: auto;" alt="Image" src="https://github.com/user-attachments/assets/70a970a7-6be1-432e-b787-6e414b9af6f6" />
</div>   

### EMR 최적화
- Caching을 활용해 Lazy Evaluation 때문에 발생하는 중복 API 호출을 개선했습니다.
- 필요없는 컬럼을 filter후 작업 하여 처리해야하는 데이터양을 줄였습니다.
- API 호출 시 Partition별로 Multithreading을 적용해 호출 시간을 개선했습니다.
### Trouble Shooting
- 외부 API 호출 문제
  - 초기에 GPT API를 호출 시 빠르게 완료하기 위해 Multithreading을 적용하였는데 많은 스레드를 만들어 호출시 block 당했습니다.
  - 때문에 이를 해결하기 위해 스레드의 수를 5개로 제한하고 추후 API 호출 실패 시 데이터 변환의 견고함을 유지하기 위해 Gemini API를 추가로 구현했습니다.
- Encoding 문제
  - 로컬에서 실행하는 환경과 달리 Airflow가 작업을 관리하게 되면서 한글을 포함한 변수에 대해 잘 동작하지 않았습니다.
  - 로컬 환경과 달리 Airflow에서는 기본적으로 모든 변수를 문자열로 전달하게 되면서 EMR 코드 상에서 사용하기 위해서는 base64, json에 기반한 decoding을 적용해야했습니다.
### runner_emr.py
    로컬에 존재하는 EMR Script를 지정한 S3에 업로드 후 Spark Job을 EMR Serverless에 제출하는 코드입니다
    EMR 환경에서 동작확인을 쉽게 할 수 있습니다
## Lambda
아래 `Lambda` 함수는 프로젝트 진행 과정에서 `Airflow`로 전환 하면서 사용하지 않게 된 코드 입니다.
### lambda_task_monitor.py
    각 source에서 개별적으로 크롤링을 하기 때문에 작업이 끝남을 람다가 감지하기 위해 구현한 코드입니다.
    이를 위해 각 크롤러는 크롤링 완료 후 종료 전 .done 형태의 파일을 저장합니다.
    해당 코드로 3분 주기로 EventBridge에 의해 실행되는 람다는 지정된 경로를 확인하고 모두 있다면 .done파일을 삭제해 초기화합니다.
### lambda_emr_news_trigger.py
    이전에 구현한 코드에서 발전시켜 크롤링 작업이 끝남을 감지했을 때 EMR Serverless에 Spark Job을 제출까지 완료하는 코드입니다.
    EventBridge가 주기적으로 호출하는 것에서 S3 event notifications를 활용해 이벤트 발생 시 해당 작업을 수행했습니다.
    성공적으로 작업을 제출하기 위해서는 해당 Lambda가 권한을 전달 해 줄 수 있는 passRole 권한을 가져야 합니다
### 전환 이유
- Spark Job 중복 제출
  - 뉴스 크롤러의 경우 모두 정적 크롤링으로 데이터를 수집하기 때문에 작업시간이 비슷했습니다. 
    때문에 거의 동시에 S3에 작업 완료 파일이 기록되었고, 
    S3 이벤트에 의해 크롤러의 개수만큼 호출된 람다가 각각 EMR에 작업을 제출해 같은 데이터에 대해 여러번 작업이 제출 되는 상황이 발생했습니다.
    이를 해결하기 위해서는 하나의 람다가 확인하고 있을 때 Block-Release 구현을 해주어햐 했습니다.
    또한 크롤러를 추가구현할 때마다 trigger lambda를 추가로 수정해야하는 불편함이 발생해 관리하는것이 어렵다고 여겨 Airflow로 전환하게 되었습니다.