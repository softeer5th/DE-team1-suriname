from datetime import datetime
from typing import List

import requests
from bs4 import BeautifulSoup

from base_crawler import BaseCrawler
from type.news_crawler import NewsRequest, NewsResponse


class SBSCrawler(BaseCrawler):
    def __init__(self, request : NewsRequest):
        super().__init__(request, source="SBS")
        self.page_base_url = (
            "https://searchapi.news.sbs.co.kr/search/news?" +
            "searchField=all&sectionCd=01%2C02%2C03%2C07%2C08%2C09%2C14&collection=news_sbs&limit=10"
        )
        self.news_base_url = "https://news.sbs.co.kr/news/endPage.do?plink=ARTICLE&cooper=SBSNEWSSEARCH"

    def get_search_result(self) -> List[NewsResponse]:
        # 페이지 선택
        news_input = self.request
        news_output_total = []
        page_offset = 0
        while True:
            news_list = self.get_page_content(news_input, page_offset)
            if len(news_list) != 0:
                news_output = self.get_news_content(news_list)
                if len(news_output) != 0:
                    print(f'{len(news_output)} news pages extracted')
                    news_output_total.extend(news_output)
            else:
                break
            page_offset += 10
        return news_output_total

    def get_page_content(self, news_input:NewsRequest, page_offset:int)->List[dict]:
        news_list=[]
        params = {
            'offset': page_offset,
            'startDate': news_input['start_time'].strftime("%Y-%m-%d"),
            'endDate': news_input['end_time'].strftime("%Y-%m-%d"),
            'query': news_input['keyword']
        }
        print(params)
        page_response = requests.get(self.page_base_url, params=params)
        if page_response.status_code == 200:
            news_list = page_response.json()['news_sbs']
        return news_list

    def get_news_content(self, news_list:List[dict])->List[NewsResponse]:
        news_output:List[NewsResponse] = []
        for news in news_list:
            news_title = news['TITLE']
            news_summary = news['REDUCE_CONTENTS']
            news_url = self.news_base_url + f'news_id={news["DOCID"]}'

            news_response = requests.get(news_url)
            if news_response.status_code == 200:
                news_soup = BeautifulSoup(news_response.text, "html.parser")

                news_date_str = news_soup.select(".date_area > span")[0].text
                news_date = datetime \
                            .strptime(news_date_str, "%Y.%m.%d %H:%M")
                print(news_date)
                if self.request['start_time'] <= news_date < self.request['end_time']:
                    news_body = " ".join(
                        news_soup.select_one("div.main_text > div.text_area").text.split()
                    )
                    news_output.append(
                        NewsResponse(
                            post_time=news_date,
                            title=news_title,
                            content=news_body,
                            source=self.source,
                            link=news_url,
                            keyword=self.request["keyword"]
                        )
                    )
        return news_output