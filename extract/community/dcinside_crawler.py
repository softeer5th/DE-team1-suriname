import os
import sys
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import time
import pandas as pd
from tempfile import mkdtemp
import urllib.parse
import itertools
import pytz

from type.community_crawler import CommunityRequest, CommunityResponse

MAX_PAGE_ACCESS = 2 # 한 페이지에 크롤링을 시도하는 최대 횟수
WAIT_TIME = 1 # 페이지 로드를 기다리는 시간


# 크롬 드라이버 경로 설정
chrome_driver_path = "/opt/chrome-driver/chromedriver-linux64/chromedriver" # lambda
# chrome_driver_path = "Users/admin/softeer/chromedriver-mac-arm64/chromedriver" # 로컬

# 크롬 경로 설정
chrome_path = "/opt/chrome/chrome-linux64/chrome"

# 크롬 옵션 설정
chrome_options = Options()
chrome_options.binary_location = chrome_path # Chrome 실행 경로 지정
chrome_options.add_argument("--headless")  # 헤드리스 모드
chrome_options.add_argument("--disable-gpu")  # GPU 비활성화
chrome_options.add_argument("--window-size=1920x1080")  # 화면 크기 설정
chrome_options.add_argument("--no-sandbox")  # 샌드박스 비활성화
chrome_options.add_argument("--disable-dev-shm-usage")  # /dev/shm 사용 비활성화
chrome_options.add_argument("--disable-dev-tools")
chrome_options.add_argument("--no-zygote")
chrome_options.add_argument("--single-process")
chrome_options.add_argument(f"--user-data-dir={mkdtemp()}")
chrome_options.add_argument(f"--data-path={mkdtemp()}")
chrome_options.add_argument(f"--disk-cache-dir={mkdtemp()}")
chrome_options.add_argument("--remote-debugging-pipe")
chrome_options.add_argument("--verbose")

class DCInsideCrawler:
    def __init__(self, request: CommunityRequest):
        self.request = request
        self.start_time = request["start_time"]
        self.end_time = request["end_time"]

    def init_driver(self):
        service = Service(chrome_driver_path) # lambda 전환 시 바꿔야 함
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(10)  # 타임아웃 시간 설정 (10초)
        return driver
    
    def start_crawling(self, num_processes=1):
        driver = self.init_driver()

        base_url = "https://gall.dcinside.com/board/lists?id=car_new1"
        # cur_search_url = self._get_start_url(driver, base_url)
        cur_search_url = base_url

        total_df = pd.DataFrame(
            columns=['post_time', 'title', 'content', 'view_count', 'like_count', 'source', 'link']
        )

        while True: # start_datetime이 될 때까지 반복
            next_search_url = self._get_next_search_url(driver, cur_search_url)
            batch_post_urls, stop_flag = self._get_batch_post_urls(driver, cur_search_url)

            print(f"[INFO] 배치 크롤링 시작\n")
            batch_df = self._get_batch_post_contents_df(driver, batch_post_urls, num_processes)
            print(f"[INFO] 배치 크롤링 종료\n")

            total_df = pd.concat([total_df, batch_df])
            last_post_datetime = None

            if len(batch_df) != 0:
                last_post_datetime = batch_df.iloc[-1]['post_time']
                # print(batch_df.iloc[-1])

            if stop_flag:
                print("[INFO] 전체 크롤링 종료\n")
                break

            cur_search_url = next_search_url

        driver.quit()

        # self.start_time, self.end_time을 Pandas Timestamp로 변환
        start_time_utc = pd.Timestamp(self.start_time).tz_localize('UTC')
        end_time_utc = pd.Timestamp(self.end_time).tz_localize('UTC')

        total_df['Datetime'] = pd.to_datetime(total_df['post_time']).dt.tz_localize('UTC')

        # 비교 연산이 올바르게 동작하도록 수정
        total_df = total_df[(total_df['Datetime'] >= start_time_utc) & (total_df['Datetime'] <= end_time_utc)]
        total_df = total_df.sort_values(by=['Datetime']).drop(['Datetime'], axis=1)
        return total_df
    
    def _get_url_soup(self, driver, url):
        driver.get(url)
        time.sleep(WAIT_TIME)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        return soup


    def _get_start_url(self, driver, search_url):
        driver.get(search_url)
        time.sleep(WAIT_TIME)
        # 종료 날짜로 이동
        end_time_str = self.end_time.strftime("%Y-%m-%d")
        driver.execute_script(f'document.getElementById("calendarInput").value = "{end_time_str}";')
        driver.execute_script('document.querySelector(".btn_blue.small.fast_move_btn").click();')
        time.sleep(WAIT_TIME)
        return driver.current_url
    
    def _get_next_search_url(self, driver, search_url):
        for _ in range(MAX_PAGE_ACCESS):
            try:
                soup = self._get_url_soup(driver, search_url)
                search_next_element = soup.select_one('div.bottom_paging_box.iconpaging a.sp_pagingicon.page_next')
                next_search_url = "https://gall.dcinside.com" + search_next_element.get('href')
                return next_search_url
            except Exception as e:
                print(f"[WARN] 다음 검색 링크 획득 재시도 - {e} - {search_url}")
        print(f"[ERROR] 다음 검색 링크 획득 실패 - {search_url}")
        return None
    
    def _get_batch_post_urls(self, driver, search_url):
        batch_post_urls_with_datetime = []
        stop_flag = False

        for page in itertools.count(start=1, step=1):    
            paged_search_url = search_url + f"&page={page}"
            post_urls_with_datetime = self._get_post_urls_with_datetime_from_post_list(driver, paged_search_url)
            # 검색 내용이 없거나 이전 크롤링 내용과 같으면 정지
            if not post_urls_with_datetime or (batch_post_urls_with_datetime and post_urls_with_datetime[-1][0] == batch_post_urls_with_datetime[-1][0]):
                break
            batch_post_urls_with_datetime.extend(post_urls_with_datetime)
            # start datetime보다 일찍 작성된 게시글을 불러왔으면 정지
            if batch_post_urls_with_datetime and pd.to_datetime(batch_post_urls_with_datetime[-1][1]) < self.start_time:
                stop_flag = True
                break
        batch_post_urls = [
            url
            for url,datetime in batch_post_urls_with_datetime
            if self.start_time <= pd.to_datetime(datetime) <= self.end_time
        ]

        return batch_post_urls, stop_flag

    ### 게시글 목록의 한 페이지에 나타난 게시글의 링크와 datetime들을 크롤링
    def _get_post_urls_with_datetime_from_post_list(self, driver, url):
        post_urls = []

        for _ in range(MAX_PAGE_ACCESS):
            try:
                prefix = "https://gall.dcinside.com"
                soup = self._get_url_soup(driver, url)
                title_elements = soup.select("tr.ub-content.us-post td.gall_tit.ub-word")
                datetime_elements = soup.select("tr.ub-content.us-post td.gall_date")
                post_urls = [
                    (prefix + title.find('a').get('href'), datetime.get('title'))
                    for title,datetime in zip(title_elements, datetime_elements)
                    if title.find('a')
                ]
            except Exception as e:
                print(f"[WARN] 게시글 목록 크롤링 재시도 - {e} - {url}\n")

            if post_urls:
                print(f"[INFO] 게시글 목록 크롤링 성공 - {url}\n")
                break
        else:
            print(f"[ERROR] 게시글 목록 크롤링 실패 - {url}\n")

        return post_urls
    
    def _get_batch_post_contents_df(self, driver, urls, num_processes=1):  # 기본적으로 단일 프로세스로 설정
        if not urls:
            return pd.DataFrame()

        post_contents = []
        
        # 순차적으로 크롤링 수행
        for url in urls:
            post_content = self._get_single_post_content(driver,url)
            post_contents.append(post_content)

        # None 값 제거 후 DataFrame 생성
        post_contents = [content for content in post_contents if content is not None]

        post_contents_df = pd.DataFrame(
            post_contents,
            columns=['post_time', 'title', 'content', 'view_count', 'like_count', 'source', 'link']
        )
        
        return post_contents_df


     ### 하나의 게시글에서 내용 크롤링
    def _get_single_post_content(self, driver, url):
        # 본문 크롤링
        def _get_post_body(soup):
            write_div = soup.select_one("div.write_div")
            body = ''
            if write_div is not None:
                # 특정 클래스의 요소들을 제거
                excluded_classes = ['imgwrap', 'og-div']
                for excluded_class in excluded_classes:
                    for element in write_div.find_all(class_=excluded_class):
                        element.extract()
                # write_div 요소 내부의 모든 p와 div 태그의 텍스트를 가져오기
                body_elements = write_div.get_text(separator="\n", strip=True)
            dc_app = body_elements.endswith("- dc official App")
            if dc_app:
                body_elements = body_elements[:-len("- dc official App")].strip()
            return body_elements
        
        
        # 추천/비추천 크롤링
        def _get_post_up_down(soup):
            try:
                up = int(soup.select_one("div.up_num_box p.up_num").text.replace(',', ''))
            except (AttributeError, ValueError):
                up = 0
            try:
                down = int(soup.select_one("div.down_num_box p.down_num").text.replace(',', ''))
            except (AttributeError, ValueError):
                down = 0
            return up, down
        
        ##########################################################################################
        title = None
        post_time = None
        view_count = 0
        like_count = 0
        body = None

        for _ in range(MAX_PAGE_ACCESS):
            try:
                soup = self._get_url_soup(driver, url)
                
                # 제목 크롤링
                title_element = soup.select_one('h3.title.ub-word span.title_subject')
                title = title_element.text if title_element else "Untitled"

                # 게시 날짜 크롤링 및 변환
                date_element = soup.select_one('div.fl span.gall_date')
                if date_element:
                    date, time_str = date_element.text.split()
                    post_time = datetime.strptime(f"{date} {time_str}", "%Y.%m.%d %H:%M:%S")
                else:
                    post_time = datetime.utcnow()

                # 조회수 크롤링 (정수 변환)
                try:
                    _, view_count, _, _, _, _ = soup.select("div.fr")[1].text.split()
                    view_count = int(view_count.replace(',', ''))
                except (ValueError, IndexError):
                    view_count = 0

                # 본문 크롤링
                body = _get_post_body(soup)
                if not body:
                    raise Exception("본문 크롤링 실패")

                # 추천 / 비추천 크롤링
                like_count, _ = _get_post_up_down(soup)
                
                if like_count is None:
                    raise Exception("추천 수 크롤링 실패")

                print(f"[INFO] 게시글 크롤링 완료 - {url}\n")
                break
            except Exception as e:
                print(f"[WARN] {e} - 게시글 크롤링 재시도 - {url}\n")
        else:
            print(f"[ERROR] 게시글 크롤링 실패 - {url}\n")
        
        return CommunityResponse(
            post_time=post_time,
            title=title,
            content=body,
            view_count=view_count,
            like_count=like_count,
            source='dcinside',
            link=url,
        )
