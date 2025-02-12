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
import re
from tempfile import mkdtemp

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))) # 로컬에서 테스트 할 시 주석 해제 필요
from type.community_crawler import CommunityRequest, CommunityResponse

BASE_URL = "https://www.bobaedream.co.kr"


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


class BobaeCrawler:
    def __init__(self, request: CommunityRequest):
        self.request = request
        self.start_time = request["start_time"]
        self.end_time = request["end_time"]
        self.keyword = request["keyword"]

    def init_driver(self):
        service = Service(chrome_driver_path) # lambda 전환 시 바꿔야 함
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(10)  # 타임아웃 시간 설정 (10초)
        return driver
    
    def extract_article_data(self, request, title, post_url):
        driver = self.init_driver()
        try:
            driver.get(post_url)
            print(f"Accessing: {post_url} - {title}") # 접속 확인 출력
            time.sleep(0.2)
            post_page_source = driver.page_source
            post_soup = BeautifulSoup(post_page_source, "html.parser")
            body_content = post_soup.find('div', {'class': 'bodyCont'})
            body_text = body_content.get_text(strip=True) if body_content else ''

            date_time_element = post_soup.find('span', {'class': 'countGroup'})
            if date_time_element:
                em_elements = date_time_element.find_all('em', {'class': 'txtType'})
                date_time_text = date_time_element.get_text(strip=True)
                date_time_match = re.search(r'(\d{4}\.\d{2}\.\d{2})\s*\(\w+\)\s*(\d{2}:\d{2})', date_time_text)

                viewCount_str = em_elements[0].get_text(strip=True) if len(em_elements) > 0 else 0
                viewCount = int(viewCount_str.replace(',', ''))
                likeCount_str = em_elements[1].get_text(strip=True) if len(em_elements) > 1 else 0
                likeCount = int(likeCount_str.replace(',', ''))
                post_time = datetime.strptime(date_time_match.group(1) + ' ' + date_time_match.group(2), "%Y.%m.%d %H:%M")

                if post_time < self.start_time:
                    return 'STOP' # 시작 날짜 이전의 게시물 무시
                if not (self.start_time <= post_time <= self.end_time):
                    return None # 지정된 날짜 범위 밖의 게시물 무시

            comments = post_soup.find_all('dd', {'id': lambda x: x and x.startswith('small_cmt_')})
            comment = [c.get_text(strip=True) for c in comments]

            return CommunityResponse(
                post_time=post_time,
                title=title,
                content=body_text,
                comment=comment,
                viewCount=viewCount,
                likeCount=likeCount,
                source='bobaedream',
                link=post_url
            )
        except TimeoutException:
            print(f"Timeout: {post_url} - {title}")
            return None
        finally:
            driver.quit()

    def extract_articles_from_page(self, page_source):
        soup = BeautifulSoup(page_source, 'html.parser')
        articles = soup.find_all('tr', {'itemscope': '', 'itemtype': 'http://schema.org/Article'})

        if not articles:
            return []
        
        article_data = []
        for article in articles:
            title_element = article.find('a', {'class': 'bsubject'})
            if title_element:
                title = title_element.get_text(strip=True)
                post_url = BASE_URL + title_element['href']
                article_data.append((post_url, title))
        return article_data
    
    def process_page(self, request, page, search_url):
        driver = self.init_driver()
        driver.get(search_url)
        time.sleep(0.2)
        page_source = driver.page_source
        driver.quit()
        article_data_list = self.extract_articles_from_page(page_source)
        return [(post_url, title, self.start_time, self.end_time) for post_url, title in article_data_list]
    
    def save_to_csv(self, request, dataframe, result_dir):
        result_file = os.path.join(result_dir, f'{self.start_time}_{self.end_time}_bobaedream_{self.keyword}.csv')
        dataframe.to_csv(result_file, index=False, encoding='utf-8-sig')
            

 

if __name__ == "__main__":
    # 명령줄 인수를 받아서 처리
    if len(sys.argv) != 4:
        print("Usage: python bobae_crawler.py <keyword> <start_time> <end_time>")
        sys.exit(1)


    keyword = sys.argv[1]
    start_time_str = sys.argv[2]
    end_time_str = sys.argv[3]

    try:
        start_time = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M")
        end_time = datetime.strptime(end_time_str, "%Y-%m-%dT%H:%M")
    except ValueError:
        print("Error: Invalid datetime format. Use YYYY-MM-DDTHH:MM")
        sys.exit(1)

    # 요청 객체 생성
    request = CommunityRequest(keyword=keyword, start_time=start_time, end_time=end_time)

    # 크롤러 객체 생성
    crawler = BobaeCrawler(request)

    # 결과 저장 디렉토리 설정
    result_dir = "data"
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)

    # 크롤링 실행
    current_page = 1
    df = pd.DataFrame()
    stop_crawling = False

    # 보배드림 검색 URL 구성
    base_url = "https://www.bobaedream.co.kr/list?code=national&s_cate=&maker_no=&model_no=&or_gu=10&or_se=desc&s_selday=&pagescale=70&info3=&noticeShow=&s_select=Body&s_key=&level_no=&vdate=&type=list&page=1"
    keyword = request['keyword']

    while not stop_crawling:
        search_url = f"{base_url}&s_key={keyword}&page={current_page}"
        # 게시글 목록 가져오기
        article_data_list = crawler.process_page(request, current_page, search_url)

        if not article_data_list:
            break  # 더 이상 게시글이 없으면 크롤링 종료

        for post_url, title, start_time, end_time in article_data_list:
            # 게시글 상세 정보 크롤링
            data = crawler.extract_article_data(request, title, post_url)

            if data == 'STOP':
                stop_crawling = True
                break
            elif data is None:
                print("Skip")
            elif data:
                df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
                print("Data extracted")

        # 일정 개수마다 저장 후 데이터프레임 초기화
        if len(df) >= 100:
            crawler.save_to_csv(request, df, result_dir)
            df = pd.DataFrame()

        current_page += 1

    # 남은 데이터 저장
    if not df.empty:
        crawler.save_to_csv(request, df, result_dir)

    print("Crawling completed successfully.")
