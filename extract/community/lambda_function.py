import io
from datetime import datetime
import json
import boto3
import pandas as pd
import pyarrow
import pyarrow.parquet as pq

from bobae.bobae_crawler import BobaeCrawler
from type.community_crawler import CommunityRequest, CommunityResponse
from conf import BUCKET_NAME, BOBAEDREAM_URL

def upload_df_to_s3(df, bucket_name, object_name):
    s3 = boto3.client('s3')

    # 데이터프레임을 parquet로 변환하여 메모리에서 처리
    parquet_buffer = io.BytesIO()

    # Pyspark에서 datetime이 깨지기 때문에 us 단위로 변환하여 저장
    table = pyarrow.Table.from_pandas(df=df)
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
        
        community = 'bobaedream'
        keyword = request['keyword']
        start_datetime = request['start_time']
        end_datetime = request['end_time']


        ### 파라미터 확인 ###
        # community = event.get('community')
        # keyword = event.get('keyword')
        # start_datetime = datetime.strptime(event.get('start_time_str'), "%Y-%m-%dT%H:%M")
        # end_datetime = datetime.strptime(event.get('end_time_str'), "%Y-%m-%dT%H:%M")


        # if not community: return {'statusCode': 400, 'body': json.dumps('Error: community is not specified.')}
        if not keyword: return {'statusCode': 400, 'body': json.dumps('Error: Keyword is not specified.')}
        if not start_datetime: return {'statusCode': 400, 'body': json.dumps('Error: Start datetime is not specified.')}
        if not end_datetime: return {'statusCode': 400, 'body': json.dumps('Error: End datetime is not specified.')}
        
        

        ### 크롤링 ###
        crawler = BobaeCrawler(request)
        results = []
        current_page = 1
        stop_crawling = False

        while not stop_crawling:
            search_url = f"{BOBAEDREAM_URL}&s_key={keyword}&page={current_page}"
            article_data_list = crawler.process_page(request, current_page, search_url)

            if not article_data_list:
                break  # 더 이상 게시글이 없으면 크롤링 종료

            for post_url, title, start_datetime, end_datetime in article_data_list:
                data = crawler.extract_article_data(request, title, post_url)

                if data == 'STOP':
                    stop_crawling = True
                    break
                elif data:
                    results.append(data)  # CommunityResponse 객체를 JSON으로 변환

            current_page += 1

        # 결과를 DataFrame으로 변환
        df = pd.DataFrame(results)

        if df.empty:
            return {'statusCode': 500, 'body': json.dumps(f"[ERROR] No data collected for {keyword}.")}
        
        ### S3 업로드 ###
        object_key = f"data/community/{start_datetime.strftime('%Y-%m-%d %H:%M:%S')}_{end_datetime.strftime('%Y-%m-%d %H:%M:%S')}/{start_datetime.strftime('%Y-%m-%d %H:%M:%S')}_{end_datetime.strftime('%Y-%m-%d %H:%M:%S')}_{community}_{keyword}.parquet"
        upload_result, msg = upload_df_to_s3(df, BUCKET_NAME, object_key)
        if upload_result == False:
            return {'statusCode': 500, 'body': json.dumps(f"[ERROR] Failed to load at S3\n{msg}")}
        else:
            done_key = f"data/community/marker/{community}.done"
            upload_df_to_s3(pd.DataFrame([{'keyword': keyword, 'start_time': start_datetime, 'end_time': end_datetime}]), BUCKET_NAME, done_key)

        return {'statusCode': 200, 'body': json.dumps(msg)}

    except ValueError as ve:
        return {'statusCode': 400, 'body': json.dumps(f"[ERROR] {str(ve)}")}
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps(f"[ERROR] Unknown error occured. {str(e)}")}