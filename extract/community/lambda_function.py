import io
from datetime import datetime
import json
import boto3
import pandas as pd
import pyarrow
import pyarrow.parquet as pq

from bobae_crawler import BobaeCrawler
from dcinside_crawler import DCInsideCrawler
from type.community_crawler import CommunityRequest, CommunityResponse
from conf import BUCKET_NAME, BOBAEDREAM_URL

def upload_df_to_s3(df, bucket_name, object_name):
    s3 = boto3.client('s3')

    # 데이터프레임을 parquet로 변환하여 메모리에서 처리
    parquet_buffer = io.BytesIO()
    # Pyspark에서 datetime이 깨지기 때문에 us 단위로 변환하여 저장
    table = pyarrow.Table.from_pandas(df=df, preserve_index=False)
    pq.write_table(table, parquet_buffer, coerce_timestamps='us')
    parquet_buffer.seek(0)

    try:
        s3.upload_fileobj(Fileobj=parquet_buffer, Bucket=bucket_name, Key=object_name)
        msg = (
            f"Total {len(df)} post are crawled.\n"
            + f"The data is successfully loaded at [{bucket_name}:{object_name}].\n"
        )
        return True, msg
    except Exception as e:
        return False, str(e)
    

def lambda_handler(event, context):
    try:
        # 요청 객체 생성
        request = CommunityRequest(keyword=event.get('keyword'), 
                                   start_time=datetime.strptime(event.get('start_time_str'), "%Y-%m-%dT%H:%M"), 
                                   end_time=datetime.strptime(event.get('end_time_str'), "%Y-%m-%dT%H:%M")) 
        
        community = event.get('community')
        keyword = request['keyword']
        start_time = request['start_time']
        end_time = request['end_time']


        if not community: return {'statusCode': 400, 'body': json.dumps('Error: community is not specified.')}
        if not keyword: return {'statusCode': 400, 'body': json.dumps('Error: Keyword is not specified.')}
        if not start_time: return {'statusCode': 400, 'body': json.dumps('Error: Start datetime is not specified.')}
        if not end_time: return {'statusCode': 400, 'body': json.dumps('Error: End datetime is not specified.')}

        # 크롤러 선택 및 실행
        if community == 'bobaedream':
            crawler = BobaeCrawler(request)
        elif community == 'dcinside':
            crawler = DCInsideCrawler(request)
        else:
            return {'statusCode': 400, 'body': json.dumps(f"Error: Unsupported community '{community}'")}
        
        df = crawler.start_crawling()

        if df.empty:
            return {'statusCode': 500, 'body': json.dumps(f"[ERROR] No data collected for {request['keyword']}.")}

        # S3 업로드
        object_key = f"data/community/{start_time.strftime('%Y-%m-%d-%H-%M-%S')}_{end_time.strftime('%Y-%m-%d-%H-%M-%S')}/{keyword}_{community}.parquet"

        upload_result, msg = upload_df_to_s3(df, BUCKET_NAME, object_key)

        if not upload_result:
            return {'statusCode': 500, 'body': json.dumps(f"[ERROR] Failed to load at S3\n{msg}")}

        # 크롤링 완료 마커 업로드
        done_key = f"data/community/marker/{community}.done"
        upload_df_to_s3(pd.DataFrame([{'keyword': request['keyword'], 'start_time': request['start_time'], 'end_time': request['end_time']}]), BUCKET_NAME, done_key)

        return {'statusCode': 200, 'body': json.dumps(msg)}
    
    except ValueError as ve:
        return {'statusCode': 400, 'body': json.dumps(f"[ERROR] {str(ve)}")}
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps(f"[ERROR] Unknown error occurred. {str(e)}")}