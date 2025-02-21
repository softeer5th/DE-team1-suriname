import os
import sys
from datetime import datetime
from bs4 import BeautifulSoup
import time
import pandas as pd
from tempfile import mkdtemp
import requests
import pyarrow
import pyarrow.parquet as pq
import boto3
import io
from concurrent.futures import ThreadPoolExecutor, as_completed

from type.community_crawler import CommunityRequest, CommunityResponse

MAX_PAGE_ACCESS = 2 # 한 페이지에 크롤링을 시도하는 최대 횟수
WAIT_TIME = 1 # 페이지 로드를 기다리는 시간


class DCInsideCrawler:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def load_links_from_csv(self, filename):
        df = pd.read_csv(filename)
        return df['link'].tolist()

    def get_urls():
        urls = []
        for i in range(12228, 12785+1):
            url = "https://gall.dcinside.com/board/lists/?id=car_new1&page=" + str(i)
            urls.append(url)

        i = 0
        data = []
        for url in urls:
            # 5개마다 2초씩 쉬기
            if i % 5 == 0:
                i = 0
                time.sleep(2)
            else:
                i += 1
            
            # 요청 시 User-Agent 헤더 넣기(권장)
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            }
            response = requests.get(url, headers=headers)
            print(f"[ Fetching page ] : {url}")
            
            if response.status_code == 200:
                # 인코딩 문제 발생 시
                response.encoding = response.apparent_encoding
                soup = BeautifulSoup(response.text, "html.parser")
                
                # 모든 tr 태그 중 class="ub-content us-post"인 요소 선택
                rows = soup.select("tr.ub-content.us-post")
                for row in rows:
                    # 각 tr 내부에서 td.gall_tit.ub-word 안의 첫 번째 a 태그 선택
                    a_tag = row.select_one("td.gall_tit.ub-word a")
                    if a_tag:
                        href = a_tag.get("href")
                        # 링크가 상대경로이면 도메인 추가
                        if href and href.startswith("/board"):
                            href = "https://gall.dcinside.com" + href
                        data.append(href)
            else:
                print("ERROR : ", response.status_code)
        
        df = pd.DataFrame(data, columns=['link'])
        df.to_csv('links.csv', index=False, encoding='utf-8')  # CSV로 저장

    def _get_post_body(self, soup):
        write_div = soup.select_one("div.write_div")
        if write_div is not None:
            # 특정 클래스의 요소들을 제거
            excluded_classes = ['imgwrap', 'og-div']
            for excluded_class in excluded_classes:
                for element in write_div.find_all(class_=excluded_class):
                    element.extract()
            # write_div 요소 내부의 모든 p와 div 태그의 텍스트를 가져오기
            body_elements = write_div.get_text(separator="\n", strip=True)
        try :    
            dc_app = body_elements.endswith("- dc official App")
            if dc_app:
                body_elements = body_elements[:-len("- dc official App")].strip()
            return body_elements
        except :
            return ' '
    
    def _get_post_up_down(self, soup):
        try:
            up = int(soup.select_one("div.up_num_box p.up_num").text.replace(',', ''))
        except (AttributeError, ValueError):
            up = 0
        try:
            down = int(soup.select_one("div.down_num_box p.down_num").text.replace(',', ''))
        except (AttributeError, ValueError):
            down = 0
        return up, down

    
    def start_crawling(self, start_id, end_id):
        data = []
        now_id = start_id
        try :
            j = 0
            for id in range(start_id, end_id+1):
                now_id = id
                if j % 5 == 0:
                    j = 0
                    time.sleep(2)
                else :
                    j = j + 1

                url = links[id]

                response = requests.get(url, headers=self.headers)
                print("[ Fetching page ID ]  : {page}".format(page = url))

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")

                    date_element = soup.select_one('div.fl span.gall_date')
                    if date_element:
                        date, time_str = date_element.text.split()
                        post_time = datetime.strptime(f"{date} {time_str}", "%Y.%m.%d %H:%M:%S")
                    else:
                        post_time = datetime.utcnow()

                    title_element = soup.select_one('h3.title.ub-word span.title_subject')
                    title = title_element.text if title_element else "Untitled"

                    # 조회수 크롤링 (정수 변환)
                    try:
                        _, view_count, _, _, _, num_comments = soup.select("div.fr")[1].text.split()
                        view_count = int(view_count.replace(',', ''))
                    except (ValueError, IndexError):
                        view_count = 0

                    # 본문 크롤링
                    body = self._get_post_body(soup)
                    if not body:
                        body = ' '

                    # 추천 / 비추천 크롤링
                    like_count, _ = self._get_post_up_down(soup)
                    
                    if like_count is None:
                        like_count = 0

                    print(f"[INFO] 게시글 크롤링 완료 - {url}\n")
                
                    data.append(
                        CommunityResponse(
                            post_time=post_time,
                            title=title,
                            content=body,
                            view_count=view_count,
                            like_count=like_count,
                            source='dcinside',
                            link=url,
                    ))
                else :
                    print("Request Error {status}".format(status = response.status_code))
            
            return data
        except :
            print("임시저장 : (start)(end)(now)", start_id, end_id, now_id)
            return data

    # def upload_df_to_s3(df, bucket_name, object_name):
    #     s3 = boto3.client('s3')

    #     # 데이터프레임을 parquet로 변환하여 메모리에서 처리
    #     parquet_buffer = io.BytesIO()
    #     # Pyspark에서 datetime이 깨지기 때문에 us 단위로 변환하여 저장
    #     table = pyarrow.Table.from_pandas(df=df, preserve_index=False)
    #     pq.write_table(table, parquet_buffer, coerce_timestamps='us')
    #     parquet_buffer.seek(0)

    #     try:
    #         s3.upload_fileobj(Fileobj=parquet_buffer, Bucket=bucket_name, Key=object_name)
    #         msg = (
    #             f"Total {len(df)} post are crawled.\n"
    #             + f"The data is successfully loaded at [{bucket_name}:{object_name}].\n"
    #         )
    #         return True, msg
    #     except Exception as e:
    #         return False, str(e)
        

    def save_df_to_parquet(df, filename='output.parquet'):
        df.to_parquet(filename, engine='pyarrow')  


    def start_crawling_multithreaded(self, start_id, end_id, max_workers=2):
        data = []

        total_ids = end_id - start_id + 1
        chunk_size = (total_ids + max_workers - 1) // max_workers

        chunks = []
        for i in range(max_workers):
            chunk_start = start_id + i * chunk_size
            chunk_end = min(end_id, chunk_start + chunk_size - 1)
            if chunk_start <= end_id:
                chunks.append((chunk_start, chunk_end))

        data = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.start_crawling, start, end): (start, end)
                for (start, end) in chunks
            }

            for future in as_completed(futures):
                result = future.result()
                if result:
                    data.extend(result)

        total_df = pd.DataFrame(
            data,
            columns=['post_time', 'title', 'content', 'view_count', 'like_count', 'source', 'link']
        )

        table = pyarrow.Table.from_pandas(total_df, preserve_index=False)
        pq.write_table(
            table,
            f"{start_id}~{end_id}.parquet",
            coerce_timestamps='us'
            )
               

crawler = DCInsideCrawler()
# crawler.start_crawling(9521786, 9559471)

links = crawler.load_links_from_csv('links.csv')
crawler.start_crawling_multithreaded(12001, 13000)
# crawler.start_crawling_multithreaded(0, 21211)