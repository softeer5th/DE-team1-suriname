from typing import TypedDict
from datetime import datetime

# 입력 데이터의 타입 정의 (키: 시작시간, 끝시간, 키워드)
class NewsRequest(TypedDict):
    start_time : datetime
    end_time : datetime
    keyword : str

# 출력 데이터의 타입 정의 (키: 게시날짜, 기사제목, 기사본문, 출처, 링크)
class NewsResponse(TypedDict):
    post_time : datetime
    title : str
    content : str
    source : str
    link : str
    keyword : str
