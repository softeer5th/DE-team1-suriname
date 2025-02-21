from type.community_crawler import CommunityRequest, CommunityResponse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from tempfile import mkdtemp
from datetime import datetime
import pandas as pd
import time

class FemcoCrawler:
    def __init__(self, request:CommunityRequest = None, is_lambda=True):
        self.request = request
        self.is_lambda = is_lambda
        self.driver = self.init_driver()
        self.article_base_url = "https://www.fmkorea.com"
        self._page_search_base_url = "https://www.fmkorea.com/index.php?mid=car&listStyle=list&page="
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        }
        self.source = "femco"

    def init_driver(self):
        options = Options()
        options.add_argument("--headless")  # 헤드리스 모드
        if self.is_lambda:
            chrome_driver_path = "/opt/chrome-driver/chromedriver-linux64/chromedriver" # lambda
            chrome_path = "/opt/chrome/chrome-linux64/chrome"
            options.binary_location = chrome_path
            options.add_argument("--disable-gpu")  # GPU 비활성화
            options.add_argument("--window-size=1920x1080")  # 화면 크기 설정
            options.add_argument("--no-sandbox")  # 샌드박스 비활성화
            options.add_argument("--disable-dev-shm-usage")  # /dev/shm 사용 비활성화
            options.add_argument("--disable-dev-tools")
            options.add_argument("--no-zygote")
            # options.add_argument("--single-process")
            options.add_argument(f"--user-data-dir={mkdtemp()}")
            options.add_argument(f"--data-path={mkdtemp()}")
            options.add_argument(f"--disk-cache-dir={mkdtemp()}")
            options.add_argument("--remote-debugging-pipe")
            options.add_argument("--verbose")
        else:
            chrome_driver_path = "/opt/homebrew/bin/chromedriver"
        service = Service(chrome_driver_path) # lambda 전환 시 바꿔야 함
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(10)  # 타임아웃 시간 설정 (10초)
        return driver

    def start_crawling(self)->pd.DataFrame:
        page = 1
        results = []
        flag = False
        while True:
            page_result = self.start_crawling_by_range(page, page, False)
            if flag == True and len(page_result) == 0:
                break
            if len(page_result) != 0:
                flag = True
                results.extend(page_result)
            page += 1
        df = pd.DataFrame(results,columns=["post_time", "title", "content", "view_count", "like_count", "source", "link"])
        return df


    def start_crawling_by_range(self, start_page:int, end_page:int, is_page_search=True)->list:
        self.driver = self.init_driver()
        url_list = self.get_url_list(start_page, end_page, is_page_search=is_page_search)
        res_list = []
        for url in url_list:
            self.driver.get(url)
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            time.sleep(2)
            try:
                top_element = soup.select_one("div.top_area.ngeb")
                post_time = datetime.strptime(top_element.select_one("span.date.m_no").text, "%Y.%m.%d %H:%M")
                if is_page_search:
                    if self.request["start_time"] <= post_time < self.request["end_time"]:
                        res_item = self.get_contents_from_url(soup, url)
                        res_list.append(res_item)
                        print("save:", res_item["link"] )
                    else:
                        print("skip:", url)
                else:
                    res_item = self.get_contents_from_url(soup, url)
                    res_list.append(res_item)
                    print("save:", res_item["link"])
            except Exception as e:
                print("parsing error: ", e)
                break
        return res_list

    def get_contents_from_url(self, soup, url:str)->CommunityResponse:
        top_element = soup.select_one("div.top_area.ngeb")
        post_time = datetime.strptime(top_element.select_one("span.date.m_no").text, "%Y.%m.%d %H:%M")
        post_title = top_element.select_one("h1.np_18px > span").text
        post_content = soup.select_one("article").text

        post_info = elements = soup.select("div.side.fr span b")
        post_view_count = int(post_info[0].text)
        post_like_count = int(post_info[1].text)
        source = "femco"
        post_link = url
        res_item:CommunityResponse = {
            "post_time": post_time,
            "title": post_title,
            "content": post_content,
            "view_count": post_view_count,
            "like_count": post_like_count,
            "source": source,
            "link": post_link
        }
        return res_item
    def get_url_list(self, start_page=1, end_page=1, is_page_search=False)->list:
        """선택한 페이지 범위의 모든 게시글 링크 list 반환"""
        search_page = start_page
        url_list = []
        while True:
            search_url = self._page_search_base_url + str(search_page)
            print(search_url)
            page_url_list = self._get_url(search_url)
            if len(page_url_list) != 0:
                url_list.extend(page_url_list)
            search_page += 1
            if search_page > end_page:
                break
        return url_list

    def _get_url(self, search_url:str)->list:
        """해당 페이지의 모든 게시글 링크 list 반환"""
        s_time = self.request["start_time"].time()
        e_time = self.request["end_time"].time()
        if e_time == datetime(2024,1,1,0,0).time():
            e_time = datetime(2024,1,1,23,59).time()
        self.driver.get(search_url)
        time.sleep(4)
        page_source = self.driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        article_list = soup.select("a.hx")
        time_list = [
            datetime.strptime(x.text.strip("\t"), "%H:%M").time()
            for x in soup.select("tbody > tr:not([class]) > td.time")
        ]

        filtered_articles = [
        self.article_base_url + article["href"]
        for article, post_time in zip(article_list, time_list)
        if s_time <= post_time < e_time
        ]
        return filtered_articles