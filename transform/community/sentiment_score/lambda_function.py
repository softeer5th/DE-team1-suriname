import os
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
rds_database = os.getenv('RDS_DB')
api_key = os.getenv('OPENAI_API_KEY')
s3 = boto3.client('s3')

db_url = f"postgresql://{rds_user}:{rds_password}@{rds_host}/{rds_database}"
engine = create_engine(db_url)

# def read_parquet_from_s3(bucket_name, start_time, end_time): # parquet 저장되는 형식, 경로에 따라 바뀌어아 햘듯

def generate_test_df():
    data = {
        'car_model': ['제네시스', '제네시스'],
        'accident': ['급발진', '급발진'],
        'post_time': ['2025-02-15T21:21:00.000Z', '2025-02-15T20:09:00.000Z'],
        'title': ['전에 누구더라 창문에 팔걸치는분이', '제네시스 쥬기는 기능'],
        'content': ['나 연봉 1억 넘는다니까웃기고 있네이러던데자기는 제네시스타고나는 존나 썩은 13년형 모하비 타니까내가 존나 없어보이나물론 존나 일이 안되면 8천까지도 떨어지지누구나 다 그런거 아님?내가 직장인 과장이라니까진짜 과장인줄 아나나참무시하는것도 가지가지네 *_*그형은 어디갓냐', '경계선 지능장애 병신은 못사난 일시불로 샀는데 ㅋㅋㅋ'],
        'comment': ['원래 기억은 과장되기 쉽죠,언제 과장달지ㅠㅠ,과장하지마세요 ㄷㄷ,진짜 부자여도 적당한 국산차 타는 분들 있습니다차로 사람 무시하고 판단하다니,차가 모든걸 말해주진 않죠부자분들중귀찮아서 가까운데서 해결하기위해 국산차 그리고 편의를 위해서만 쓰시는 분들도 많습니다,체어맨,힘듬니다만 ㄷㄷ,련봉 1억 ㄷㄷ,국밥 좀 먹겧슴미다,한달 연봉 1억,그게 목표임미다만몸이 열개는 되어야 ㄷㄷ,난 반장인데,어딜가도 반장이 짱임,부대찌개 한그릇도 못사주면서 연봉자랑하지 마세혀;; ㄷㄷ', '저는 먹을걸 못사고있습니다,형은 괜찮음,안사 병신아 ㅗ그딴거 타서 뭐하냐 ㅗ,일시불은 당신이 샇잖아계약자는 거래현장으로 반드시 돌아온다할부 살려내복돌이는 안샇네밀크는 왜 보여줬어혹시,거래,@영잡닉어,ㅋㅋㅋㅋㅋ,경계선지능장애 병신은 못사잊지마'],
        'viewcount': ['177', '527'],
        'likecount': ['2', '2']
    }
    df = pd.DataFrame(data)
    return df

def add_sentiment_score_column(df):
    df['sentiment_score'] = None
    return df

def get_sentiment_score(client, df):
    scores = []
    for _, row in df.iterrows():
        car_model = row['car_model']
        accident = row['accident']
        title = row['title']
        content = row['content']
        comment = row['comment']

        prompt = f"""
        너는 자동차 관련 글에서 특정 차량과 사고 유형에 대한 감성 분석을 수행하는 AI야.
        주어진 데이터를 기반으로 **차량 모델과 사고 유형**(예: '{car_model}' + '{accident}')에 대한 전체적인 감성을 분석해줘.

        각 행에는 제목(title), 본문(content), 댓글(comment)들이 포함되어 있어.
        댓글은 ,로 구분되어 있어.
        이 모든 요소를 종합하여 **해당 차량과 사고 유형에 대한 감성 점수**를 -1(매우 부정적)에서 1(매우 긍정적)까지의 범위로 숫자로 평가해줘.
        0에 가까울수록 중립적이며, -1에 가까울수록 부정적, 1에 가까울수록 긍정적인거야.

        아래는 분석할 데이터야:

        ---
        **차량 모델:** {car_model}  
        **사고 유형:** {accident}  
        **제목:** {title}  
        **본문:** {content}  
        **댓글:** {comment}  
        
        ---
        근거는 3줄로 요약해줘.
        - 결과는 아래 format을 따라서 JSON으로 반환.
        {{
            "sentiment_score": "[감성 점수]",
            "reason": "[근거]"
        }}
        ```
        """

        try:
            completion = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )

            raw_output = completion.choices[0].message.content.strip()
            left = raw_output.find('{')
            right = raw_output.rfind('}')
            summary_content = raw_output[left:right+1]
            summary_json = json.loads(summary_content)

            sentiment_score = float(summary_json.get('sentiment_score', 0))
            scores.append(sentiment_score)
            reason = summary_json.get('reason', None)

            print(f"{car_model} {accident} 감성 점수: {sentiment_score}")
            print(f"근거: {reason}\n\n")

        except Exception as e:
            print(f"Error in parsing response: {e}")
            scores.append(None)
        
    df["sentiment_score"] = scores
    return df
    
def calculate_average_sentiment(df):
    sentiment_df = df.groupby(['car_model', 'accident'], as_index=False)['sentiment_score'].mean()
    return sentiment_df
    

def update_rds_table(sentiment_df, schema_name, table_name):
    try:
        with engine.begin() as connection:
            update_query = text(f"""
                UPDATE {schema_name}.{table_name}
                SET sentiment_score = :sentiment_score
                WHERE car_model = :car_model AND accident = :accident;
            """)
            
            update_values = [
                {
                    'car_model': row['car_model'],
                    'accident': row['accident'],
                    'sentiment_score': row['sentiment_score']
                }
                for _, row in sentiment_df.iterrows()
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

        # read_parquet_from_s3('s3-bucket', start_time, end_time)
        df = generate_test_df()
        print("Dataframe generated.")

        if df.empty:
            print("No crawled data")
            return {
                'statusCode': 200,
                'body': 'S3 community table was not updated, but it ran successfully'
            }
        
        df = add_sentiment_score_column(df)
        print("Sentiment score column added.")

        client = OpenAI(api_key=api_key)

        # GPT API 호출하여 sentiment score 계산
        df = get_sentiment_score(client, df)

        if df is None:
            return {
                'statusCode': 500,
                'body': 'Internal Server Error: GPT API call failed.'
            }
        
        # 감성 점수 평균 계산
        sentiment_df = calculate_average_sentiment(df)

        # RDS 테이블 업데이트
        update_rds_table(sentiment_df, 'test', 'accumulated_table')
        print("RDS table updated successfully.")

        return {
            'statusCode': 200,
            'body': json.dumps('RDS table updated successfully.')
        }

    except Exception as e:
        print(f"Error: {e}")
        return {'statusCode': 400, 'body': json.dumps(f'Error: {str(e)}')}