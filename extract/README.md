# Extract
뉴스와 커뮤니티를 크롤링 합니다. 뉴스 크롤러는 1시간 배치로 실행되고, 커뮤티니 크롤러는 뉴스 데이터를 통해 구한 통계값이 임계치를 넘은 경우 뉴스 파이프라인 다음으로 실행됩니다.

## 폴더 구조
- [community/](https://github.com/softeer5th/DE-team1-suriname/tree/main/extract/community/) : 2개의 커뮤니티에서 데이터를 추출하는 파일들 입니다.
- [news/](https://github.com/softeer5th/DE-team1-suriname/tree/main/extract/news/) : 5개의 뉴스사에서 데이터를 추출하는 파일들 입니다.

## 데이터 추출하기
- 커뮤니티와 뉴스 크롤러 모두 AWS Lambda 환경에서 실행됩니다. 각 사이트마다 하나의 Lambda 환경을 사용하게 됩니다. 따라서 2개의 커뮤니티와 4개의 뉴스사를 크롤링 하기 때문에 총 6개의 Lambda 환경이 사용됩니다. 
- 커뮤니티, 뉴스 크롤러 모두 Multithreading이 적용되어 I/O Bound인 크롤링 작업을 효율적으로 처리할 수 있도록 구현하였습니다. 
- 두 크롤러 모두 Airflow 환경에서 동작되며, News Dag trigger가 발생할 경우 4개의 Lambda 환경이 생성되어 뉴스 크롤링이 시작되고, Community Dag trigger가 발생할 경우 2개의 Lambda 환경이 생성되어 커뮤니티 크롤링이 시작되게 됩니다.
- 크롤링이 끝나게 되면 S3에 데이터가 저장됩니다.


### community
<div style="text-align: center;">
    <img style="width: 80%; height: auto;"  alt="Image" src="https://github.com/user-attachments/assets/a0d763c4-daa2-46c0-ad52-58c16105d1e3" />
</div>

- Selenium을 활용한 동적크롤링과 BeautifulSoup을 활용한 정적크롤링의 조합으로 크롤링이 진행됩니다. 
- 커뮤니티 크롤러는 2개의 자동차 관련 커뮤니티 사이트에서 배치 시간동안 올라온 모든 글을 크롤링 합니다. 

### News
<div style="text-align: center;">
    <img style="width: 80%; height: auto;" alt="Image" src="https://github.com/user-attachments/assets/10cac6ee-878b-4aa0-b465-afcd2bbcdc50" />
</div>

- Beautiful soup을 활용한 정적크롤링으로 진행됩니다.
- 뉴스 크롤러는 각 사이트에서 이슈 키워드(미리 지정해둔)를 검색하고, 배치 시간동안 올라온 뉴스기사들을 크롤링 합니다.


## Trouble Shooting
### Lambda 환경에서의 동적 크롤링
- Lambda에서는 각종 라이브러리를 설치해주기 위해서 Layer를 사용하는데 Layer의 최대 용량이 정해져있었습니다. 하지만 동적 크롤링을 위해 Selenium 및 Chrome driver를 설치해주게 될 경우 Layer의 용량이 너무 커져 등록할 수 없는 문제가 발생하였습니다.
- 이를 해결하기 위해 Docker로 실행 환경 및 코드를 담은 이미지를 만들어 ECR에 업로드하고 Lambda는 이러한 도커 이미지를 바탕으로 실행 환경을 만들도록 구현하였습니다.

### EventBridge -> S3 Notification -> Airflow
- 뉴스 크롤러 4개의 Lambda들이 끝나서 S3에 데이터가 모두 저장되게 되면, 다음 Transform 을 실행시켜야 하기 때문에 모두 끝난 타이밍을 감지해야 합니다.
- 구현 초기에는 Airflow를 사용하지 않았습니다.
- 따라서 이를 구현하기 위해 EventBridge 방식을 사용하였습니다. 하지만 이는 3분마다 S3의 이벤트를 수집해야 했고, 배치시간이 아니었을때도 이벤트를 수집했어야 했습니다.
- 위와 같은 문제를 해결하기 위해 S3 Notification을 활용하였습니다. S3 Notification은 S3 파일이 생성되는 것을 감지해 모든 크롤링 결과 파일이 생성되면, 다음 작업을 호출하였습니다.
- 하지만 두 개의 뉴스 크롤러가 미세한 시간차로 거의 동시에 종료될 경우, 2번의 S3 Notification이 실행되는데, 거의 동시에 끝났기 때문에 파일을 확인할 때는 두 번 모두 모든 크롤링 결과 파일이 완전히 생성된 것으로 판단하여 후속 작업이 중복으로 호출되는 문제가 발생하였습니다.
- 위와 같은 문제와 transform, load 과정에서도 플로우 처리 및 종료 감지문제를 원활히 해결하기 위해 Airflow를 도입하였습니다. 
- Airflow에서는 모든 Lambda task가 끝나야 후속 task를 실행하도록 구현하여 위 문제를 해결할 수 있었습니다.



## 협업
- 팀원들끼리 각자 작업 후 통합할 때 편리하도록 crawler의 부모 클래스를 미리 구현하였습니다. [base_crawler.py](https://github.com/softeer5th/DE-team1-suriname/tree/main/extract/news/crawler/base_crawler.py)를 바탕으로  이를 상속받아 각각의 뉴스 크롤러를 만들도록 구현하였습니다. 
- [type](https://github.com/softeer5th/DE-team1-suriname/tree/main/extract/news/type)을 통해 Request, Response type을 사전에 통일하여, 각각의 함수들이 서로 같은 형태의 데이터를 입력받고, 출력할 수 있도록 구현하였습니다.