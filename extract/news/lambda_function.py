from datetime import datetime
import sys
from type.news_crawler import NewsRequest
from sbs_crawler import SBSCrawler
from kbs_crawler import KBSCrawler
from yna_crawler import YNACrawler
import json
import threading
import boto3

NEWS_SOURCES = ["kbs", "sbs", "yna"]


def lambda_handler(event, context):
    try:
        keyword = event.get('keyword')
        start_time = datetime.strptime(event.get('start_time_str'), "%Y-%m-%dT%H:%M")
        end_time = datetime.strptime(event.get('end_time_str'), "%Y-%m-%dT%H:%M")
        request: NewsRequest = {
            "keyword": keyword,
            "start_time": start_time,
            "end_time": end_time
        }

        if not keyword: return {'statusCode': 400, 'body': json.dumps('Error: Missing Arguments - Keyword.')}
        if not start_time: return {'statusCode': 400, 'body': json.dumps('Error : Missing Arguments - Start_time.')}
        if not end_time: return {'statusCode': 400, 'body': json.dumps('Error : Missing Arguments - End_time.')}

        threads = []
        results = {}

        for source in NEWS_SOURCES:
            if source == "kbs":
                kbs_crawler = KBSCrawler(request=request)
                thread = threading.Thread(target=kbs_crawler.run)
                thread.start()
                threads.append(thread)

            if source == "sbs":
                sbs_crawler = SBSCrawler(request=request)
                thread = threading.Thread(target=sbs_crawler.run)
                thread.start()
                threads.append(thread)

            if source == "yna":
                yna_crawler = YNACrawler(request=request)
                thread = threading.Thread(target=yna_crawler.run)
                thread.start()
                threads.append(thread)

        for thread in threads:
            thread.join()

        trigger_emr()

        return {'statusCode': 200, 'body': json.dumps(results)}
    except ValueError as valueError:
        return {'statusCode': 400, 'body': json.dumps(str(valueError))}
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps(f"Error: Unknown error occured. {str(e)}")}


def trigger_emr():
    pass
