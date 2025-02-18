from datetime import datetime
import sys
from type.news_crawler import NewsRequest
from sbs_crawler import SBSCrawler
from kbs_crawler import KBSCrawler
from yna_crawler import YNACrawler
from ytn_crawler import YTNCrawler
import json
import concurrent.futures

KEYWORD_CARMODEL = ["제네시스", "아이오닉", "아반떼"]
CRAWLER_MAP = {
    "kbs": KBSCrawler,
    "sbs": SBSCrawler,
    "yna": YNACrawler,
    "ytn": YTNCrawler
}

def lambda_handler(event, context):
    try:
        source = event.get('source')
        start_time = datetime.strptime(event.get('start_time_str'), "%Y-%m-%dT%H:%M")
        end_time = datetime.strptime(event.get('end_time_str'), "%Y-%m-%dT%H:%M")

        if not start_time: return {'statusCode': 400, 'body': json.dumps('Error : Missing Arguments - Start_time.')}
        if not end_time: return {'statusCode': 400, 'body': json.dumps('Error : Missing Arguments - End_time.')}
        if not source : return  {'statusCode': 400, 'body': json.dumps('Error : Missing Arguments - Source.')}

        crawler = CRAWLER_MAP[source]

        # 멀티스레딩 실행
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(KEYWORD_CARMODEL)) as executor:
            futures = [
                executor.submit(crawler(NewsRequest(keyword=keyword, start_time=start_time, end_time=end_time)).run)
                for keyword in KEYWORD_CARMODEL
            ]

        # 모든 작업 완료 대기
        concurrent.futures.wait(futures)

        return {'statusCode': 200, 'body': json.dumps("All news crawlers done")}
    except ValueError as valueError:
        return {'statusCode': 400, 'body': json.dumps(str(valueError))}
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps(f"Error: Unknown error occured. {str(e)}")}
