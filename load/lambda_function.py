import json
import boto3
import psycopg2
import pandas as pd
import io
import pyarrow.parquet as pq
# AWS 클라이언트 설정
s3_client = boto3.client("s3")

def get_s3_file(bucket_name, file_key):
    """S3에서 파일을 가져오는 함수"""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        content = response["Body"].read().decode("utf-8")  # 파일 내용을 문자열로 변환
        return content
    except Exception as e:
        return str(e)

def query_rds(query):
    """RDS(PostgreSQL)에서 쿼리 실행하는 함수"""
    try:
        # PostgreSQL 데이터베이스 연결
        conn = psycopg2.connect(
            host=RDS_HOST,
            port=RDS_PORT,
            user=RDS_USER,
            password=RDS_PASSWORD,
            dbname=RDS_DB
        )
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()  # 모든 데이터 가져오기
        conn.close()
        return result
    except Exception as e:
        return str(e)

def lambda_handler(event, context):

    # S3에서 파일 가져오기
    # 뉴스 데이터 가져오기
    s3 = boto3.client('s3')
    bucket_name = event["BUCKET_NAME"]
    news_folder_path = 'data/news/output/' + event["BATCH_PERIOD"] + '/'
    
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
    community_folder_path = 'data/community/output/' + event["BATCH_PERIOD"] + '/'
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

    return {
        "statusCode": 200,
        "body": json.dumps({
        })
    }
