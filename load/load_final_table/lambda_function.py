import json
import boto3
import psycopg2
import pandas as pd
import io
import numpy as np 
import pyarrow.parquet as pq
import datetime

# AWS 클라이언트 설정
s3_client = boto3.client("s3")
WEIGHTS = {
    'news_score': 0.2,
    'comm_score': 0.2,
    'news_count': 0.1,
    'news_acc_count': 0.2,
    'comm_count': 0.3
}

def get_data_from_RDS(event) -> pd.DataFrame :
    try:
        with psycopg2.connect(
            dbname=event["dbname"], 
            user=event["user"], 
            password=event["password"],
            host=event["url"],
            port=event["port"]
        ) as conn:
            with conn.cursor() as cur:
                query = """
                SELECT * FROM accumulated_table
                WHERE is_issue = TRUE
                """
                cur.execute(query)
                rows = cur.fetchall()
                col_names = [desc[0] for desc in cur.description]
                df = pd.DataFrame(rows, columns=col_names)
    except Exception as e:
        print(f"Error executing query: {e}")
        df = pd.DataFrame()  
    print("RDS =======================================")
    print(df.head())
    return df


def piecewise_linear(x, lower, mid, upper):
    """
    x: 원본 값
    lower: 구간의 하한 
    mid: 구간의 중간 값
    upper: 구간의 상한 
    
    반환: x에 대해 [0,1] 범위로 스케일된 값  
    매핑: 
      - x <= lower: 0  
      - lower < x < mid: 선형 보간으로 0 ~ 0.5  
      - mid <= x < upper: 선형 보간으로 0.5 ~ 1  
      - x >= upper: 1
    """
    if x <= lower:
        return 0.0
    elif x < mid:
        return 0.5 * (x - lower) / (mid - lower)
    elif x < upper:
        return 0.5 + 0.5 * (x - mid) / (upper - mid)
    else:
        return 1.0

piecewise_linear_vec = np.vectorize(piecewise_linear)

def load_issue_score(df_community, df_news, conn, event) : 
    # 예상 범위
    # news_count 
    # 1~5개 / 5개 ~ 10개 / 10개 ~ 

    # news_acc_count
    # 10 ~ 20 / 20 ~ 30 / 30개 ~

    # community_posts
    # 0 ~ 0.5 / 0.5 ~ 1 / 1
    # 1~10 / 10 ~ 20 / 20~
    df_from_rds = get_data_from_RDS(event)
    df_scaled = df_from_rds.copy()
    df_view_table = df_from_rds.copy()

    # 정규화 
    # news_score의 범위가 0~100이기 때문에 100으로 나눠 [0,1] 변환
    df_scaled['news_score'] = df_scaled['news_score'] / 100.0
    
    # news_acc_count: 범위 10 ~ 20 / 20 ~ 30 / 30 이상
    df_scaled['news_acc_count'] = piecewise_linear_vec(df_scaled['news_acc_count'], lower=10, mid=20, upper=30)

    df_scaled['comm_score'] = piecewise_linear_vec(df_scaled['comm_score'], lower=50, mid=75, upper=100)
    
    # df_news에서 필요한 컬럼만 선택하여 df_view_table 조인
    df_scaled = df_scaled.merge(df_news[['car_model', 'accident', 'count']],
                                on=['car_model', 'accident'],
                                how='left')
    df_view_table = df_view_table.merge(df_news[['car_model', 'accident', 'count']],
                                on=['car_model', 'accident'],
                                how='left')

    # 조인 후, 해당 키에 맞는 값이 없으면 NaN이 생기므로 0으로 채움
    df_scaled['count'] = df_scaled['count'].fillna(0)
    df_view_table['count'] = df_view_table['count'].fillna(0)

    df_scaled = df_scaled.rename(columns={'count': 'news_count'})
    df_view_table = df_view_table.rename(columns={'count': 'news_count'})


    df_scaled = df_scaled.merge(df_community[['car_model', 'accident', 'count']],
                                on=['car_model', 'accident'],
                                how='left')
    df_view_table = df_view_table.merge(df_community[['car_model', 'accident', 'count']],
                                on=['car_model', 'accident'],
                                how='left')
    
    df_scaled = df_scaled.rename(columns={'count': 'comm_count'})
    df_view_table = df_view_table.rename(columns={'count': 'comm_count'})

    df_scaled['comm_count'] = df_scaled['comm_count'].fillna(0)
    df_view_table['comm_count'] = df_view_table['comm_count'].fillna(0)

    # 2. news_count: 범위 1 ~ 5 / 5 ~ 10 / 10 이상
    df_scaled['news_count'] = piecewise_linear_vec(df_scaled['news_count'], lower=1, mid=5, upper=10)

    # 4. community_posts: 범위 1 ~ 5 / 5 ~ 10 / 10 이상
    df_scaled['comm_count'] = piecewise_linear_vec(df_scaled['comm_count'], lower=1, mid=5, upper=10)

    # 가중합으로 최종 이슈 스코어 계산
    df_scaled['issue_score'] = (
        df_scaled['news_score'] * WEIGHTS['news_score'] +
        df_scaled['comm_score'] * WEIGHTS['comm_score'] +
        df_scaled['news_count'] * WEIGHTS['news_count'] +
        df_scaled['news_acc_count'] * WEIGHTS['news_acc_count'] +
        df_scaled['comm_count'] * WEIGHTS['comm_count']
    )

    cur = conn.cursor()

    # df_scaled의 각 행에 대해 issue_score 업데이트
    for idx, row in df_scaled.iterrows():
        car_model = row['car_model']
        accident = row['accident']
        issue_score = row['issue_score']
        
        update_query = """
        UPDATE accumulated_table
        SET issue_score = %s
        WHERE car_model = %s AND accident = %s;
        """
        cur.execute(update_query, (issue_score, car_model, accident))

    conn.commit()
    cur.close()

    df_view_table = df_view_table.drop('issue_score', axis=1)
    df_view_table = df_view_table.merge(df_scaled[['car_model', 'accident', 'issue_score']],on=['car_model', 'accident'], how='left')

    return df_view_table

def load_final_table(df_view_table, conn, event) :
    cur = conn.cursor()
    df_view_table = df_view_table.drop('news', axis=1)

    for idx, row in df_view_table.iterrows():
        car_model = row['car_model']
        accident = row['accident']
        issue_score = row['issue_score']
        comm_count = row['comm_count']
        news_count = row['news_count']
        start_batch_time = row['start_batch_time']
        batch_time  = event["batch_period"].split('_')[1]
        batch_time_dt = datetime.datetime.strptime(batch_time, '%Y-%m-%d-%H-%M-%S')

        insert_query = """
        INSERT INTO final_table (car_model, accident, issue_score, comm_count, news_count, start_batch_time, batch_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(insert_query, (car_model, accident, issue_score, comm_count, news_count, start_batch_time, batch_time_dt))

    conn.commit()
    cur.close()
    conn.close()
    
def load_community(df:pd.DataFrame, conn):
    cur = conn.cursor()
    for idx, row in df.iterrows():
        query = f"""
        UPDATE accumulated_table
        SET comm_score = {row['avg_comm_score']} ,
            comm_acc_count = COALESCE(comm_acc_count, 0) + {row['count']}
        WHERE car_model = '{row['car_model']}'
            AND accident = '{row['accident']}';
        """
        try:
            cur.execute(query)
        except Exception as e:
            print(f"Error executing query: {e}")
            conn.rollback()

    conn.commit()
    cur.close()
    return

def lambda_handler(event, context):

    # S3에서 파일 가져오기
    # 뉴스 데이터 가져오기
    s3 = boto3.client('s3')
    bucket_name = event["bucket_name"]
    news_folder_path = 'data/news/output/' + event["batch_period"] + '/'
    
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=news_folder_path)
    parquet_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]

    # multithreading 도입해서 개선할 수 있을듯
    dfs = []
    for file_key in parquet_files:
        print(f"Loading: {file_key}")
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        data = response['Body'].read()
        table = pq.read_table(io.BytesIO(data))
        df = table.to_pandas()
        dfs.append(df)

    if dfs :
        df_news = pd.concat(dfs, ignore_index=True)
        print("news =======================================")
        print(df_news.head())
    else : 
        df_news = pd.DataFrame(columns=['car_model', 'accident', 'count'])

    # 커뮤니티 데이터 가져오기
    community_folder_path = 'data/community/output/' + event["batch_period"] + '/'
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=community_folder_path)
    parquet_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]

    dfs = []
    for file_key in parquet_files:
        print(f"Loading: {file_key}")
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        data = response['Body'].read()
        table = pq.read_table(io.BytesIO(data))
        df = table.to_pandas()
        dfs.append(df)

    if dfs :
        df_community = pd.concat(dfs, ignore_index=True)
        print("communtiy =======================================")
        print(df_community.head())
    else :
        df_community = pd.DataFrame(columns=['car_model', 'accident', 'comm_count'])


    conn = psycopg2.connect(
        dbname= event["dbname"], 
        user= event["user"], 
        password= event["password"],
        host= event["url"],
        port= event["port"], 
    )
    load_community(df_community, conn)
    df_view_table = load_issue_score(df_community, df_news, conn, event)
    load_final_table(df_view_table, conn, event)

    return {
        "statusCode": 200,
        "body": json.dumps({
        })
    }

