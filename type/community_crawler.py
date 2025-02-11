from typing import TypedDict, List
from datetime import datetime

# 커뮤니티 댓글 타입 (키 : 내용)
class CommunityComment(TypedDict):
    content : str

# 입력 데이터의 타입 (키: 시작시간, 끝시간, 키워드)
class CommunityRequest(TypedDict):
    start_time : datetime
    end_time : str
    keyword : str

# 출력 데이터의 타입 (키: 게시날짜, 제목, 본문, 댓글, 조회수, 좋아요, 출처, 링크)
class CommunityResponse(TypedDict):
    post_time : datetime
    title : str
    content : str
    comment : List[CommunityComment]
    viewCount : int
    likeCount : int
    source : str
    link : str
