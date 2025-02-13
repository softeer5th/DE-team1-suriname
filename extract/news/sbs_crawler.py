import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import sys
from conf import BUCKET_NAME
from type.news_crawler import NewsRequest, NewsResponse
from typing import List, Optional
import io
import boto3

class SBSCrawler:
    NEWS_TAG = 'SBS'
    def __init__(self, request : NewsRequest):
        self.request = request
        self.page_base_url = (
            "https://searchapi.news.sbs.co.kr/search/news?" +
            "searchField=all&sectionCd=01%2C02%2C03%2C07%2C08%2C09%2C14&collection=news_sbs&limit=10"
        )
        self.news_base_url = "https://news.sbs.co.kr/news/endPage.do?plink=ARTICLE&cooper=SBSNEWSSEARCH"

    def get_search_result(self)-> Optional[List[NewsResponse]]:
        # 페이지 선택
        news_input = self.request
        news_output_total = []
        page_offset = 0
        while True:
            news_list = self.get_page_content(news_input, page_offset)
            news_output = self.get_news_content(news_list)
            if len(news_output) != 0:
                print(len(news_output))
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
                news_body = " ".join(
                    news_soup.select_one("div.main_text > div.text_area").text.split()
                )
                news_output.append(
                    NewsResponse(
                        post_time=news_date,
                        title=news_title,
                        content=news_body,
                        source=self.NEWS_TAG,
                        link=news_url,
                        keyword=self.request["keyword"]
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
        df.to_parquet(parquet_buffer, engine="pyarrow")
        parquet_buffer.seek(0)

        try:
            s3.upload_fileobj(Fileobj=parquet_buffer, Bucket=BUCKET_NAME, Key=f_name)
            print(
                f"[SBS] {len(df)} post are crawled.\n"
                + f"[SBS] The data is successfully loaded at [{BUCKET_NAME}:{f_name}].\n"
            )
        except Exception as e:
            print("Error : {e}".format(e = e))
    
    def run(self):
        search_result = self.get_search_result()
        f_name = f'data/news/{self.request["start_time"]}_{self.request["end_time"]}_SBS_{self.request["keyword"]}.parquet'
        self.upload_s3(search_result, f_name)

        # 로컬용
        # self.save_to_parquet(search_result, f_name)


# if __name__ == "__main__":
#     if len(sys.argv) == 4:
#         keyword = sys.argv[1]
#         start_time_str = sys.argv[2]
#         end_time_str = sys.argv[3]
#         start_time = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M")
#         end_time = datetime.strptime(end_time_str, "%Y-%m-%dT%H:%M")

#         news_input : NewsRequest = {
#             "keyword": keyword,
#             "start_time": start_time,
#             "end_time": end_time
#         }
#         sbs_crawler = SBSCrawler()
#         search_result = sbs_crawler.get_search_result(news_input)
#         sbs_crawler.save_to_parquet(search_result)
#     else:
#         print("Error : Missing Arguments | python sbs_crawler.py <keyword> <start_time> <end_time>", file=sys.stderr)