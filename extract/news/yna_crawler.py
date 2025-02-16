import json
import re
from datetime import datetime
from typing import Optional, List

import requests
from bs4 import BeautifulSoup

from base_crawler import BaseCrawler
from type.news_crawler import NewsRequest, NewsResponse


class YNACrawler(BaseCrawler):
    def __init__(self, request : NewsRequest):
        super().__init__(request, source='YNA')
        self.page_base_url = "http://ars.yna.co.kr/api/v2/search.asis?callback=Search.SearchPreCallback&ctype=A&page_size=10&channel=basic_kr"
        self.news_base_url = "https://www.yna.co.kr/view/"

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
                            source=self.source,
                            link=news_url,
                            keyword=self.request['keyword']
                        )
                    )
        return news_output

    def _response_parser(self, response: requests.Response) -> json:
        response_json_str = re.sub(r"^Search\.SearchPreCallback\(|\)$", "", response.text)
        return json.loads(response_json_str)