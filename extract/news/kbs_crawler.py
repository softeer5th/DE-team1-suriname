from datetime import datetime
from typing import List

import requests

from base_crawler import BaseCrawler
from type.news_crawler import NewsRequest, NewsResponse


class KBSCrawler(BaseCrawler):
    def __init__(self, request : NewsRequest) :
        super().__init__(request, source='KBS')

    def get_search_result(self) -> List[NewsResponse]:
        page_num = 1
        news_list = []
        while True:
            news_data_list = self.get_news_data(page_num)
            if len(news_data_list) == 0:
                break
            news_list.extend(news_data_list)
            page_num += 1

        return news_list

    def get_news_data(self, page_num) -> List[NewsResponse]:
        news_links = [] 
        try : 
            query = self.request["keyword"]
            sdate = self.request["start_time"].strftime("%Y.%m.%d")
            edate = self.request["end_time"].strftime("%Y.%m.%d")
            url = "https://reco.kbs.co.kr/v2/search?target=newstotal&keyword={query}&page={page_num}&page_size=10&sort_option=date&searchfield=all&categoryfield=&sdate={sdate}&edate={edate}&include=&exclude=&_=1739265845640".format(query = query, page_num = page_num, sdate = sdate, edate = edate)

            response = requests.get(url, headers=self.headers)
            print("fetching news links : {url}".format(url = url))

            if response.status_code == 200:
                res = response.json()
                data = res['data']
                for item in data :
                    # service_time을 datetime 형식으로 파싱
                    service_time = datetime.strptime(item["service_time"], "%Y%m%d %H%M%S")

                    if self.request["start_time"] <= service_time < self.request["end_time"] :
                        new_item = NewsResponse(
                            post_time = service_time,
                            title=item["title"],
                            content=item["contents"],
                            source="KBS",
                            link=item["target_url"],
                            keyword=self.request["keyword"]
                        )
                        news_links.append(new_item)
            else :
                print("Request Error {status}".format(status = response.status_code))

        except Exception as e:
            print("Error : {error}".format(error = e))
            return []

        return news_links
