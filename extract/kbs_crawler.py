from typing import List
import pandas as pd
from type.news_crawler import NewsRequest, NewsResponse
from datetime import datetime
import sys
import requests

# Todo
# parquet 저장

class KBSCrawler :
    def __init__(self, request : NewsRequest) :
        self.request = request

    def get_news_data(self, page_num) -> List[NewsResponse]:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        news_links = [] 
        try : 
            query = self.request["keyword"]
            sdate = self.request["start_time"].strftime("%Y.%m.%d")
            edate = self.request["end_time"].strftime("%Y.%m.%d")
            url = "https://reco.kbs.co.kr/v2/search?target=newstotal&keyword={query}&page={page_num}&page_size=10&sort_option=date&searchfield=all&categoryfield=&sdate={sdate}&edate={edate}&include=&exclude=&_=1739265845640".format(query = query, page_num = page_num, sdate = sdate, edate = edate)

            response = requests.get(url, headers=headers)
            print("fetching news links : {url}".format(url = url))

            if response.status_code == 200:
                res = response.json()
                data = res['data']
                for item in data :
                    # service_time을 datetime 형식으로 파싱
                    service_time = datetime.strptime(item["service_time"], "%Y%m%d %H%M%S")

                    # print("---------")
                    # print(self.request["start_time"])
                    # print(service_time)
                    # print(self.request["end_time"])

                    if self.request["start_time"] <= service_time <= self.request["end_time"] :
                        new_item = NewsResponse(
                            post_time = service_time,
                            title=item["title"],
                            content=item["contents"],
                            source="KBS",
                            link=item["target_url"]
                        )
                        news_links.append(new_item)
            else :
                print("Request Error {status}".format(status = response.status_code))

        except Exception as e:
            print("Error : {error}".format(error = e))
            return []

        return news_links
                 
    def run(self) :
        page_num = 1
        news_list = []
        while True :
            news_data_list = self.get_news_data(page_num)
            if len(news_data_list) == 0:
                break 
            news_list.extend(news_data_list) 
            page_num += 1
        df = pd.DataFrame(news_list, columns=["post_time", "title", "content", "source", "link"])
        df.to_csv('data.csv')

if __name__ == "__main__":
    if len(sys.argv) == 4:
        keyword = sys.argv[1]
        start_time_str = sys.argv[2]
        end_time_str = sys.argv[3]

        start_time = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M")
        end_time = datetime.strptime(end_time_str, "%Y-%m-%dT%H:%M")

        request : NewsRequest = {
            "keyword": keyword,
            "start_time": start_time,  
            "end_time": end_time 
        }

        kbs_crawler = KBSCrawler(request)
        kbs_crawler.run()
    else : 
        print("Error : Missing Arguments | python kbs_crawler.py <keyword> <start_time> <end_time>")

