# Load
각각의 transform이 끝난 데이터들을 데이터베이스 혹은 데이터 웨어하우스에 적재합니다.

## 폴더 구조
- [load_final_table/](https://github.com/softeer5th/DE-team1-suriname/tree/main/load/load_final_table/) : 뉴스와 커뮤니티 데이터의 transform이 끝난 후 두 데이터를 활용해 이슈 주의도를 계산하고 이들을 Data Warehouse(Redshift)에 적재하는 파일입니다.


- [load_news/](https://github.com/softeer5th/DE-team1-suriname/tree/main/load/load_news/) : 뉴스 데이터의 transform이 끝난 후 계산된 데이터를 RDS(PostgreSQL)에 업데이트 하는 파일입니다.

## 데이터 테이블
### accumulated_table(PostgreSQL)
<div style="text-align: center;">
    <img style="width: 80%; height: auto;"  alt="Image" src="https://github.com/user-attachments/assets/856d6262-7b86-48f2-a25d-df44a86e005d" />
</div>



- 뉴스 데이터의 transform이 끝난 데이터의 통계가 업데이트 되는 테이블입니다. 


- 테이블에는 차총(10가지) * 이슈(6가지) = 총 60개의 행이 디폴트로 삽입 되어있습니다. 


- car_model 과 accident로 primary key를 이루고 있습니다.


- 주로 UPDATE가 이뤄지는 테이블입니다.


- __해당 테이블의 데이터들은 이슈라 판단하는 임계값을 넘었는지 판단 및 이슈 여부, 알림 여부를 판단하기 위한 데이터입니다.__

### final_table(Redshift)
<div style="text-align: center;">
    <img style="width: 80%; height: auto;"  alt="Image" src="https://github.com/user-attachments/assets/7d8111d3-b725-4a29-bfcb-eb4358e1c27e" />
</div>



- 뉴스 데이터와 커뮤니티 데이터를 활용해 계산된 이슈 주의도와 배치 시간대의 뉴스, 커뮤니티 데이터가 적재되는 테이블 입니다. 


- car_model, accident, batch_time으로 primary key를 이루고 있습니다.


- 주로 데이터의 INSERT 와 SELECT가 이뤄지는 테이블입니다.


- __해당 테이블의 데이터들은 Apache Superset에서 통계 및 시계열 데이터로 활용될 데이터입니다.__

## 데이터 적재하기
### 뉴스 데이터
- EMR에서 transform이 끝난 후 S3에 저장된 뉴스 데이터를 PostgreSQL의 테이블에 업데이트 합니다.(해당 과정은 Load Lambda에서 실행됩니다.)


- 데이터들(뉴스 기사수, 점수, last_batch_time 등)은 조건에 맞는 행들에 업데이트 됩니다.


- accumulated 의 축적된 뉴스 기사의 개수(news_acc_count) 값이 설정해둔 임계값을 넘는다면 is_issue 가 true로 업데이트 되게 됩니다. 이는 해당 차종_사건(예시 : 제네시스_급발진)이 이슈화 되었다고 판단됨을 나타냅니다.


- is_issue가 true가 된 시점에는 is_alert도 true가 되어 추후 사용자에게 해당 이슈로 슬랙 알림을 보낼 때 사용됩니다. (slack 알림 후에는 is_alert는 false가 됩니다.)


- 하루동안 뉴스가 발생되지 않은 이슈는 사라진 이슈라 판단되어 is_issue가 false로 업데이트 됩니다.


### 커뮤니티 데이터
- S3에 저장되어 있는 transform이 완료된 뉴스,커뮤니티 데이터를 활용하여 해당 배치 시간의 이슈주의도를 계산 후, 통계와 함께 Redshift 테이블에 삽입됩니다.(해당 과정은 Load Lambda에서 실행됩니다.)


- 배치 시간 마다 계속해서 테이블에 데이터가 쌓이게 됩니다.


## Trouble Shooting
### 어떤 데이터 베이스를 선택할까 
- 현재는 AWS RDS(PostgreSQL)와 Redshift를 모두 사용하고 있습니다.
- 하지만 초기에는 AWS RDS만 사용하였습니다. __당시 RDS를 사용하게 된 근거는 다음과 같았습니다.__
    1. RDS에서 관리할 데이터는 is_issue, is_alert, accumulated_count와 같이 이슈인지 판단을 할 때 사용되는 데이터로써 주로 update가 이뤄지는 데이터로 OLTP 시스템이 적합하다고 판단하였습니다.


    2. RDS의 accumulated_table에는 행이 60개로 고정되어 있어 대규모 데이터가 되지 않아 데이터 웨어하우스에서 만들어주지 않아도 된다고 생각하였습니다.


    3. 당시 시각화를 충분히 구체화하지 못하였고, 단순히 시각화용 테이블(현재의 final_table)을 만들고, view table을 활용화여 시각화를 하면 되겠다고 생각하였습니다.


- 추후 Load 작업 및 시각화 작업이 구체화 되면서 시각화용 테이블이 어디에 위치해야 할지 고민이 되었고, 그 결과 Redshift를 도입하게 되었습니다. __Redshift를 도입하게 된 근거는 다음과 같습니다.__


    1. final_table의 데이터들은 transform의 결과로써, 사용시 오직 SELECT 작업 위주로 진행될 데이터이기 때문에 OLAP 시스템이 더 적합하다고 생각하였습니다. 


    2. 데이터가 배치 주기마다 쌓이게 되어, 추후 서비스가 유지될 경우 수많은 데이터가 누적되게 됩니다. 이는 분산처리가 시스템이며 열기반 데이터베이스인 Redshift를 이용하게 되면 효율적인 읽기 작업이 가능하다고 생각했습니다.


    3. 데이터 웨어하우스인 Redshift를 학습하고 사용해보고 싶었습니다.
- __최종적으로, 이슈 및 알림 여부등을 판단 할 때 사용되는 데이터는 AWS RDS, 시각화 및 시계열 데이터에서 사용되는 데이터는 Redshift에 저장하는 방식으로 구현하였습니다.__


### Redshift의 비효율성?
- 위에서 언급한 대로 현재 AWS RDS와 Redshift 모두 사용하고 있습니다.
- 하지만 고민해서 도입한 Redshift에도 몇가지 비효율성을 가져오고 있습니다.🥲


    1. 데이터가 별로 없음에도 불구하고 쿼리 실행시간이 일반적인 RDBMS에서 실행한 결과보다 매우 느렸습니다. Redshift에서는 쿼리 실행전 효율적인 실행 계획을 생성하고, 데이터 처리가 분산 처리로 이뤄지는데, 이로 인한 오버헤드가 SQL을 처리하는 시간보다 커서 발생하는 문제였습니다. 아직 대용량의 데이터가 아니기 때문에 Redshift 사용 효과를 얻을 수 없었습니다. 


    2. 아직 대용량 데이터가 아니기 때문에 PostgreSQL(RDS)에 테이블을 만들고, View table을 생성하여 시각화해도 크게 문제가 없었습니다.


    3. Redshift 비용이 매우 비쌌습니다.

    
- 학습의 의도가 없이 실제 서비스를 운영 한다고 생각한다면, 어느정도 데이터 규모가 생기기 전까지는 RDS로 통합해서 관리하고 추후 필요시 마이그레이션을 진행하는 방향으로 생각했습니다.
