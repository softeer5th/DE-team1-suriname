import os
import re
import json
import boto3
import pandas as pd
import psycopg2
from openai import OpenAI
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# rds.env 파일의 경로를 지정하여 환경 변수 불러오기
load_dotenv('.env')
# 환경 변수에서 RDS 정보 불러오기
rds_host = os.getenv('RDS_HOST')
rds_user = os.getenv('RDS_USER')
rds_password = os.getenv('RDS_PASSWORD')
rds_database = os.getenv('RDS_DATABASE')
api_key = os.getenv('OPENAI_API_KEY')

db_url = f"postgresql://{rds_user}:{rds_password}@{rds_host}/{rds_database}"
engine = create_engine(db_url)

# AWS RDS에 연결하는 함수
# def connect_to_rds():
#     try:
#         connection = psycopg2.connect(
#             host=rds_host,
#             user=rds_user,
#             password=rds_password,
#             dbname=rds_database,
#             connect_timeout=5  # PostgreSQL에서는 connect_timeout을 초 단위로 지정
#         )
#         print("Successfully connected to AWS RDS (PostgreSQL).")
#         return connection
#     except psycopg2.Error as e:
#         print(f"ERROR in connecting to RDS: {e}")
#         return None
    
def get_df_from_rds(schema_name, table_name):
    try:
        # SQL 쿼리 실행
        query = f"SELECT * FROM {schema_name}.{table_name};"
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
        print("Fetched columns:", df.columns.tolist())
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None
    
def news_issue_judgement(text, client, car_name, accident):
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "user",
                "content": f"""
                [뉴스 제목 및 본문]
                {text}

                [할 것]
                - 위의 뉴스 내용들은 현대자동차 {car_name} 모델의 {accident} 관련한 이슈, 사고를 다루고 있는 내용들이고, 한 이슈에 대한 여러 기사들을 너에게 입력해줬어.
                - 위의 뉴스 내용을 기반으로 아래 작업을 수행해줘.
                - 답변은 반드시 한글로 작성되어야 해.
                - 해당 뉴스에서 다음의 세 가지 항목을 추출해줘.
                >> 요약: 본문을 3문장으로 요약
                >> 이슈 심각도: 해당 뉴스가 현대자동차의 브랜드 이미지에 얼마나 타격을 줄만한 뉴스인지 수치화해줘.
                    - 이슈 심각도를 평가할 때 이슈 심각도를 평가할 때 주로 고려해야 하는 요소들은 다음과 같아.
                        - 현대차의 직접적 책임 여부 : 차량 결함 vs 운전자 과실
                        - 사고 규모 : 인명 피해 및 사고 차량 수
                        - 언론 및 소비자 반응 : 부정적 보도, 여론 형성 여부
                        - 법적 규제 리스크 : 리콜, 소송, 정부 개입 가능성
                    - 점수 산정 기준은 다음과 같아.
                        - 80점 ~ 100점(최악의 상황, 강력한 대응 필수)
                            - 현대차의 직접적인 책임 : 차량 결함이 명백하고 심각함
                            - 사고 규모 : 다수의 사망, 중상 사고 발생. 글로벌 뉴스화
                            - 언론 및 소비자 반응 : 부정적 여론 폭발, 불매 운동 조짐
                                - 법적 규제 리스크 : 대규모 리콜, 국가 차원의 조사 착수
                            - 60점 ~ 80점(매우 심각, 신속한 대응 필요)
                                - 현대차의 책임 가능성 높음 : 일부 논란 있지만 확실한 증거 부족
                                - 사고 규모 : 사망자는 없지만 중대 사고 발생
                                - 언론 및 소비자 반응 : 부정적 여론 확산, 브랜드 신뢰 하락 우려
                                - 법적 규제 리스크 : 리콜 가능성 높음, 당국 조사 진행
                            - 40점 ~ 60점(주의 요망, 전략적 대응 필요)
                                - 현대차의 책임 불분명 : 운전자 과실 가능성 존재
                                - 사고 규모 : 일부 차량 손상 및 경상자 발생
                                - 언론 및 소비자 반응 : 논란 있지만 대중적 관심 크지 않음
                                - 법적 규제 리스크 : 리콜 가능성 낮음, 조사는 진행될 수 있음
                            - 20점 ~ 40점(경미한 이슈, 모니터링 필요)
                                - 현대차의 책임 낮음 : 외부 요인 가능성 높음
                                - 사고 규모 : 일부 차량 문제지만 대형 사고 아님
                                - 언론 및 소비자 반응 : 국지적 논란, 이슈 확산 가능성 낮음
                                - 법적 규제 리스크 : 리콜 필요 없음, 단순 보상 차원 해결 가능
                            - 0점 ~ 20점(무시 가능, 영향 미미)
                                - 현대차의 책임 거의 없음 : 소비자 또는 외부 요인으로 판명
                                - 사고 규모 : 개별 사례이며 확대 가능성 없음
                                - 언론 및 소비자 반응 : 온라인 커뮤니티 불만 수준, 확산 가능성 적음
                                - 법적 규제 리스크 : 리콜, 조사 필요 없음
                    >> 근거 : 너가 이슈 심각도를 그렇게 매긴 이유를 설명해주면 돼.


                - 결과는 아래 format을 따라서 JSON으로 반환.
                    {{
                        "summary": "[본문 요약 결과]",
                        "issue_judgement": "[이슈 심각도 점수]",
                        "reason", "[근거]"
                    }}
                """
            }
        ],
        temperature=0.2,
    )
    try:
        raw_output = completion.choices[0].message.content.strip()
        left = raw_output.find('{')
        right = raw_output.rfind('}')
        summary_content = raw_output[left:right+1]
        summary_json = json.loads(summary_content)

        summary = summary_json.get('summary', None)
        issue_judgement = summary_json.get('issue_judgement', None)
        reason = summary_json.get('reason', None)

        print(raw_output)
        print(f"{summary} | {issue_judgement} | {reason}\n\n")
        return summary, issue_judgement, reason
    except Exception as e:
        print(f"Error in parsing response: {e}")
        return None, None, None

def update_rds_table(df, schema_name, table_name):
    try:
        with engine.begin() as connection:
            update_query = text(f"""
                UPDATE {schema_name}.{table_name}
                SET issue_score = :issue_score
                WHERE car_model = :car_model AND accident = :accident;
            """)

            update_values = [
                {"issue_score": row["issue_score"], "car_model": row["car_model"], "accident": row["accident"]}
                for _, row in df.iterrows()
            ]

            connection.execute(update_query, update_values)
        print(f"Updated {len(update_values)} rows in the RDS table.")
    
    except Exception as e:
        print(f"Error updating RDS table: {e}")
    
 


def lambda_handler(event, context):
    try:
        keyword = event.get('keyword')
        accident = event.get('accident')
        start_time = datetime.strptime(event.get('start_time_str'), "%Y-%m-%dT%H:%M")
        end_time = datetime.strptime(event.get('end_time_str'), "%Y-%m-%dT%H:%M")

        if not keyword: return {'statusCode': 400, 'body': json.dumps('Error: Missing Arguments - Keyword.')}
        if not accident: return {'statusCode': 400, 'body': json.dumps('Error: Missing Arguments - Accident.')}
        if not start_time: return {'statusCode': 400, 'body': json.dumps('Error : Missing Arguments - Start_time.')}
        if not end_time: return {'statusCode': 400, 'body': json.dumps('Error : Missing Arguments - End_time.')}

        df = get_df_from_rds('test', 'accumulated_table')
        print(f"Number of crawled news articles: {df.shape[0]}")

        if df.empty:
            print("No crawled data")
            return {
                'statusCode': 200,
                'body': 'RDS news table was not updated, but it ran successfully'
            }
        
        # is_issue == True인 행만 필터링
        df_filtered = df[df["is_issue"] == True].copy()
        print(f"Number of articles with is_issue=True: {df_filtered.shape[0]}")

        # `is_issue == True`인 데이터가 없으면 GPT API 호출 필요 없음
        if df_filtered.empty:
            print("No articles with is_issue=True, skipping GPT API calls.")
            return {
                'statusCode': 200,
                'body': json.dumps('No articles required GPT processing. RDS update skipped.')
            }

        client = OpenAI(api_key=api_key)
        
        # GPT API 호출하여 issue_score 생성 (news_issue_judgement 사용)
        def get_issue_score(row):
            _, issue_judgement, _ = news_issue_judgement(row["contents"], client, row["car_model"], row["accident"])
            return issue_judgement  # 이슈 심각도 점수만 저장
        
        df_filtered["issue_score"] = df_filtered.apply(get_issue_score, axis=1)
        print("Issue scores computed successfully.")

        # 기존 df에서 필터링된 데이터의 `issue_score` 업데이트
        df.update(df_filtered[["car_model", "accident", "issue_score"]])

        # RDS 테이블 업데이트
        update_rds_table(df_filtered, 'test', 'accumulated_table')
        print("RDS table updated successfully.")

        return {
            'statusCode': 200,
            'body': json.dumps('RDS table updated successfully.')
        }

    except Exception as e:
        print(f"Error: {e}")
        return {'statusCode': 400, 'body': json.dumps(f'Error: {str(e)}')}


