from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from utils import KST
from pydantic import BaseModel

Base = declarative_base()

# 모델 정의
class Pageview(Base):
    __tablename__ = "pageviews"
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(KST))
    url = Column(String)  # 사용자가 접속한 URL
    ip_address = Column(String) # 사용자 IP
    session_id = Column(String) # 사용자 session ID
    user_location = Column(String)  # 사용자의 지역 정보
    user_agent = Column(String)  # 사용자의 User-Agent 정보
    is_mobile = Column(Integer)  # 모바일 기기 여부
    is_pc = Column(Integer)  # PC 기기 여부
    referer_url = Column(String) # 이전 url 추가
    # reload = Column(String) # 새로고침 여부 파악

class AnchorClick(Base):
    __tablename__ = "anchor_clicks"
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(KST))
    source_url = Column(String)  # 사용자가 클릭한 링크의 출발 URL
    target_url = Column(String)  # 사용자가 클릭한 링크의 도착 URL
    ip_address = Column(String) # 사용자 IP
    session_id = Column(String) # 사용자 session ID
    user_agent = Column(String)  # 사용자의 User-Agent 정보
    is_mobile = Column(Integer)  # 모바일 기기 여부
    is_pc = Column(Integer)  # PC 기기 여부
    type = Column(String)

class WenivSql(Base):
    __tablename__ = "wenivsql_data"
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(KST))
    contents = Column(String)  # run sql 한 내용
    ip_address = Column(String) # 사용자 IP
    session_id = Column(String) # 사용자 session ID
    user_agent = Column(String)  # 사용자의 User-Agent 정보
    is_mobile = Column(Integer)  # 모바일 기기 여부
    is_pc = Column(Integer)  # PC 기기 여부

# 수집할 데이터의 모델 정의
class PageviewData(BaseModel):
    url: str
    # reload: int

class AnchorClickData(BaseModel):
    source_url: str
    target_url: str
    type:str

class WenivSqlData(BaseModel):
    contents:str
