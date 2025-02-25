# Slack 알림
- 이슈가 발생할 경우 이슈대응팀에게 슬랙 안내 메시지를 전송합니다.


- Airflow Task에서 오류가 발생할 경우 개발팀에게 슬랙 안내 메시지를 전송합니다. 빠른 오류 대응을 통해 파이프라인의 가용성을 높이고자 하였습니다.

## 폴더 구조
- [slack_alert/](https://github.com/softeer5th/DE-team1-suriname/tree/main/sns/slack_alert.py/) : 이슈가 발생할 경우 이슈대응팀에게 슬랙 메시지를 전송하는 파일입니다. 
- [slack_alert_develop/](https://github.com/softeer5th/DE-team1-suriname/tree/main/sns/slack_alert_develop.py/) : Task에서 에러가 발생할 경우 개발팀에게 슬랙 메시지를 전송하는 파일입니다. 

## 슬랙 메시지 전송
- 슬랙 메시지 전송은 AWS lambda 환경에서 실행됩니다.


- 슬랙의 Webhook URL을 이용하여 메시지를 전송합니다.

### 이슈대응팀에게 메시지 전송
- 파이프라인의 Load까지 종료되면 Slack Lambda가 실행됩니다. 


- Lambda 내부에서는 PostrgreSQL 테이블에 접근하여 is_alert가 true인 사건이 있는지 확인합니다. 


- is_alert가 true인 행이 있다면 이슈 대응팀에게 슬랙 메시지를 전송합니다.

### 개발팀에게 메시지 전송
- Airflow task 실패시 Slack 개발자 알림 Lambda가 호출됩니다.


- Airflow에서는 Task 상태, 문제 발생시간 ,로그 등을 lambda event로 전달합니다.


- Lambda 내부에서는 event의 데이터들을 개발팀에게 전송합니다. 

## Trouble Shooting
### Airflow에서 Slack 메시지 보내기
- Airflow에서 Slack 메시지를 보내는 방법으로 다양한 방법을 시도해 보았습니다.


- 첫번째, Airflow plugin에 Slack을 호출하는 모듈을 만들어 넣고, 이 모듈을 불러와 dag 파일에서 사용하는 방법입니다. 이를 시도해봤지만 plugin 관리 및 사용에서 에러가 발생하여 해당 방법을 사용하지 않았습니다. 


- 두번째, Airflow Slack 알림 패키지 사용하는 방법이 있었습니다. 이 방법은 잦은 import 오류로 사용하지 못했습니다.


- 마지막으로, Slack 메시지를 전송하는 Lambda를 만들고, Airflow task에서 on_failure_callback를 통해 오류 발생시 Lambda가 실행되도록 호출하는 방법이 있었습니다. 프로젝트에서는 이미 구축해놓은 slack Lambda가 있었기 때문에 이를 바탕으로 구축하여, Task 실패시 개발자에게 슬랙알림을 보내는 서비스를 구현할 수 있었습니다.
