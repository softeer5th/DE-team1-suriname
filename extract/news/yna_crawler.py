from datetime import datetime
from typing import Optional, List
import re
import json
import requests
from bs4 import BeautifulSoup
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from conf import *
import io
import sys
from extract.news.type.news_crawler import NewsRequest, NewsResponse

class YNACrawler(object):
    NEWS_TAG = 'YNA'

    def __init__(self, request : NewsRequest):
        self.request = request
        self.page_base_url = "http://ars.yna.co.kr/api/v2/search.asis?callback=Search.SearchPreCallback&ctype=A&page_size=10&channel=basic_kr"
        self.news_base_url = "https://www.yna.co.kr/view/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def get_search_result(self)-> Optional[List[NewsResponse]]:
        news_input = self.request
        news_output_total = []
        page_no = 1
        while True:
            news_list = self.get_page_content(news_input, page_no)
            news_output = self.get_news_content(news_list)
            if len(news_output) != 0:
                print(len(news_output))
                news_output_total.extend(news_output)
            else:
                break
            page_no += 1

        print(f'{len(news_output_total)} news pages extracted')
        return news_output_total

    def get_page_content(self, news_input: NewsRequest, page_no: int) -> List[dict]:
        news_list = []
        params = {
            'query': news_input['keyword'],
            'page_no': page_no,
            'period': 'diy',
            'from': news_input['start_time'].strftime('%Y%m%d'),
            'to': news_input['end_time'].strftime('%Y%m%d')
        }

        response = requests.get(self.page_base_url, params=params, headers=self.headers)
        if response.status_code == 200:
            response_json = self._response_parser(response)
            news_list = response_json['KR_ARTICLE']['result']

        return news_list

    def get_news_content(self, news_list: List[dict]) -> List[NewsResponse]:
        news_output:List[NewsResponse] = []
        for news in news_list:
            news_title = news['TITLE']
            news_url = f'{self.news_base_url}{news["CONTENTS_ID"]}'
            news_response = requests.get(news_url, headers=self.headers)
            if news_response.status_code == 200:
                news_soup = BeautifulSoup(news_response.text, 'html.parser')
                news_date_str = news_soup.select_one('.update-time')["data-published-time"]
                news_date = datetime.strptime(news_date_str, '%Y-%m-%d %H:%M')
                if self.request['start_time'] <= news_date <= self.request['end_time']:
                    news_body_list = news_soup.select('article.story-news.article > p:not([class])')
                    news_body = " ".join(p.text.strip() for p in news_body_list)

                    news_output.append(
                        NewsResponse(
                            post_time=news_date,
                            title=news_title,
                            content=news_body,
                            source=self.NEWS_TAG,
                            link=news_url,
                            keyword=self.request['keyword']
                        )
                    )
        return news_output

    def save_to_parquet(self, news_output:List[NewsResponse], f_name:str)->None:
        if len(news_output) != 0:
            df = pd.DataFrame(news_output, columns=["post_time", "title", "content", "source", "link","keyword"])
            df.to_parquet(f_name, engine="pyarrow")
        else:
            print("news_output is empty", file=sys.stderr)

    def upload_s3(self, news_output:List[NewsResponse], f_name:str)->None:
        s3 = boto3.client('s3')
        df = pd.DataFrame(news_output, columns=["post_time", "title", "content", "source", "link","keyword"])

        # 데이터프레임을 parquet로 변환하여 메모리에서 처리
        parquet_buffer = io.BytesIO()

        # Pyspark에서 datetime이 깨지기 때문에 us 단위로 변환하여 저장
        table = pa.Table.from_pandas(df=df)
        pq.write_table(table, parquet_buffer, coerce_timestamps='us')
        parquet_buffer.seek(0)

        try:
            s3.upload_fileobj(Fileobj=parquet_buffer, Bucket=BUCKET_NAME, Key=f_name)
            print(
                f"[{self.NEWS_TAG}] {len(df)} post are crawled.\n"
                + f"[{self.NEWS_TAG}] The data is successfully loaded at [{BUCKET_NAME}:{f_name}].\n"
            )
        except Exception as e:
            print("Error : {e}".format(e = e))

    def run(self):
        search_result = self.get_search_result()
        f_name = f'data/news/{self.request["start_time"]}_{self.request["end_time"]}/{self.request["keyword"]}_{self.NEWS_TAG}.parquet'
        self.upload_s3(search_result, f_name)

        # 로컬용
        # self.save_to_parquet(search_result, f_name)

    def _response_parser(self, response: requests.Response) -> json:
        response_json_str = re.sub(r"^Search\.SearchPreCallback\(|\)$", "", response.text)
        return json.loads(response_json_str)