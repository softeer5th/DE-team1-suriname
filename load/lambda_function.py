import json
import boto3
import psycopg2
import pandas as pd
# AWS 클라이언트 설정
s3_client = boto3.client("s3")

def (df, connect):
    
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
    return {
        "statusCode": 200,
        "body": json.dumps({
        })
    }
