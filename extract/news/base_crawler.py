from io import BytesIO

import pandas as pd
import sys
import pyarrow as pa
import pyarrow.parquet as pq
from conf import BUCKET_NAME, MARKER_PATH
from type.news_crawler import NewsRequest, NewsResponse
from typing import List
import io
import boto3
import json
from abc import ABC, abstractmethod

class BaseCrawler(ABC):
    def __init__(self, request: NewsRequest, source: str):
        self.request = request
        self.source = source
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    @abstractmethod
    def get_search_result(self) -> List[NewsResponse]:
        """각 크롤러 클래스가 페이지에 맞게 크롤링하여 조건에 맞는 모든 검색결과를 반환"""
        pass

    def save_to_parquet(self, news_output: List[NewsResponse], f_name: str) -> None:
        if len(news_output) != 0:
            df = pd.DataFrame(news_output, columns=["post_time", "title", "content", "source", "link", "keyword"])
            df.to_parquet(f_name, engine="pyarrow")
        else:
            print("news_output is empty", file=sys.stderr)

    def upload_s3(self, news_output: List[NewsResponse], f_name: str) -> None:
        s3 = boto3.client('s3')
        df = pd.DataFrame(news_output, columns=["post_time", "title", "content", "source", "link", "keyword"])

        # 데이터프레임을 parquet로 변환하여 메모리에서 처리
        parquet_buffer = io.BytesIO()

        # Pyspark에서 datetime이 깨지기 때문에 us 단위로 변환하여 저장
        table = pa.Table.from_pandas(df=df)
        pq.write_table(table, parquet_buffer, coerce_timestamps='us')
        parquet_buffer.seek(0)

        try:
            s3.upload_fileobj(Fileobj=parquet_buffer, Bucket=BUCKET_NAME, Key=f_name)
            print(
                f"[{self.source}] {len(df)} post are crawled.\n"
                + f"[{self.source}] The data is successfully loaded at [{BUCKET_NAME}:{f_name}].\n"
            )
            s3.upload_fileobj(Fileobj=self._get_trigger_obj(), Bucket=BUCKET_NAME,
                              Key=f"{MARKER_PATH}{self.source}.json")
        except Exception as e:
            print("Error : {e}".format(e=e))

    def _get_trigger_obj(self)->BytesIO:
        json_data = {
            "source": self.source,
            "start_time": self.request["start_time"].strftime("%Y-%m-%d %H:%M"),
            "end_time": self.request["end_time"].strftime("%Y-%m-%d %H:%M"),
            "status": "success"
        }
        json_bytes = json.dumps(json_data).encode("utf-8")
        file_obj = io.BytesIO(json_bytes)
        return file_obj

    def run(self):
        search_result = self.get_search_result()
        f_name = f'data/news/{self.request["start_time"]}_{self.request["end_time"]}/{self.request["keyword"]}_{self.source}.parquet'
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

#         request : NewsRequest = {
#             "keyword": keyword,
#             "start_time": start_time,
#             "end_time": end_time
#         }

#         crawler = BaseCrawler(request, source="base")
#         crawler.run()
#     else :
#         print("Error : Missing Arguments | python base_crawler.py <keyword> <start_time> <end_time>")