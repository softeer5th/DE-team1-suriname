from datetime import datetime
from typing import List

import requests
from base_crawler import BaseCrawler
from type.news_crawler import NewsRequest, NewsResponse
from bs4 import BeautifulSoup

class YTNCrawler(BaseCrawler):
    def __init__(self, request : NewsRequest) :
        super().__init__(request, source='YTN')

    def get_search_result(self) -> List[NewsResponse]:
        urls = []
        page_num = 1
        while True :
            urls_per_page = self.get_news_url(page_num)
            if len(urls_per_page) == 0:
                break 
            urls.extend(urls_per_page)
            page_num += 1

        return self.get_news_data(urls)
    
    def get_news_url(self,page) -> List:
        query = self.request["keyword"]
        sdate = self.request["start_time"].strftime("%Y%m%d")
        edate = self.request["end_time"].strftime("%Y%m%d")

        url = "https://www.ytn.co.kr/search/index.php?q={query}&x=0&y=0&se_date=3&ds={sdate}&de={edate}&target=0&mtarget=0&page={page}".format(query=query,sdate=sdate, edate=edate,page=page)
        print("fetching news links : {url}".format(url = url))
        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            links = [a['href'] for a in soup.select('.search_news_list a')]
            return links 
        else :
            return []
            

    def get_news_data(self, urls : List) -> List[NewsResponse]:
        news_data = [] 

        for url in urls:
            response = requests.get(url)
            print("fetching news links : {url}".format(url = url))

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                
                date_str = soup.select_one('#czone > div.news_title_wrap.inner > div > div.date').text.strip()
                # "오전"과 "오후" 처리
                if "오후" in date_str:
                    date_str = date_str.replace("오후", "").strip()
                    dt = datetime.strptime(date_str, "%Y.%m.%d. %I:%M.")
                    post_time = dt.replace(hour=dt.hour + 12 if dt.hour < 12 else dt.hour)
                elif "오전" in date_str:
                    date_str = date_str.replace("오전", "").strip()
                    post_time = datetime.strptime(date_str, "%Y.%m.%d. %H:%M.")
                else:
                    post_time = datetime.strptime(date_str, "%Y.%m.%d. %H:%M") 

                title = soup.select_one('#czone > div.news_title_wrap.inner > h2 > span').text.strip()
                content = soup.select_one('#CmAdContent').text.strip()
                source = "YTN"
                link = url
                keyword = self.request["keyword"]

                if self.request["start_time"] <= post_time < self.request["end_time"] :
                    new_item = NewsResponse(
                        post_time = post_time,
                        title=title,
                        content=content,
                        source=source,
                        link=link,
                        keyword=keyword
                    )
                    
                    news_data.append(new_item)
        return news_data
