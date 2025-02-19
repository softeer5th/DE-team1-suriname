import json
import boto3
import psycopg2
import pandas as pd
import io
import numpy as np 
import pyarrow.parquet as pq
# AWS 클라이언트 설정
s3_client = boto3.client("s3")

def load_news(df:pd.DataFrame, batch_period:str, issue_threshold:int, conn):
    cursor = conn.cursor()
    start_batch_time, last_batch_time = batch_period.split('_')
    for idx, row in df.iterrows():
        car_model = row['car_model']
        accident = row['accident']
        batch_count = row['count']
        news_json = row['news']
        news_score = row['avg_news_score']
        
        merge_batch_query = f"""
        UPDATE accumulated_table
        SET start_batch_time = CASE
            WHEN news_acc_count = 0 THEN TO_TIMESTAMP('{start_batch_time}', 'YYYY-MM-DD-HH24-MI-SS')
            ELSE start_batch_time
        END,
        news_acc_count = news_acc_count + {batch_count} ,
        last_batch_time = TO_TIMESTAMP('{last_batch_time}', 'YYYY-MM-DD-HH24-MI-SS') ,
        news = jsonb_set(
            news,
            '{{news}}',
            COALESCE(news->'news', '[]'::jsonb) || '{news_json}'::jsonb ) ,
        news_score = {news_score}
        WHERE car_model = '{car_model}'
            AND accident = '{accident}';
        """
        try:
            cursor.execute(merge_batch_query)
        except Exception as e:
            print(f"Error executing query: {e}")
            conn.rollback()


    
    clear_alert_column_query = f"""
        UPDATE accumulated_table
        SET is_alert = FALSE;
    """

    clear_dead_issue_query = f"""
        UPDATE accumulated_table
        SET news_acc_count = 0,
            is_issue = FALSE,
            is_alert = FALSE,
            start_batch_time = NULL,
            last_batch_time = NULL,
            news_score = 0,
            comm_score = 0,
            issue_score = 0,
            comm_acc_count = 0,
            news = '{{"news":[]}}'::jsonb
        WHERE CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' - TO_TIMESTAMP('{start_batch_time}', 'YYYY-MM-DD-HH24-MI-SS') > '1 day';
    """

    set_issue_query = f""" 
        UPDATE accumulated_table
        SET is_issue = TRUE,
        is_alert = TRUE
        WHERE news_acc_count >= {issue_threshold} and is_issue = FALSE;
    """
    try:
        cursor.execute(clear_alert_column_query)
        # cursor.execute(clear_dead_issue_query)
        cursor.execute(set_issue_query)
    except Exception as e:
        print(f"Error executing query: {e}")
        conn.rollback()

    conn.commit()
    cursor.close()
    
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

    batch_period = event["batch_period"]
    issue_threshold = event["threshold"]
    conn = psycopg2.connect(
        dbname= event["dbname"], 
        user= event["user"], 
        password= event["password"],
        host= event["url"],
        port= event["port"], 
    )
    load_news(df_news,batch_period,issue_threshold, conn)

    return {
        "statusCode": 200,
        "body": json.dumps({
        })
    }

