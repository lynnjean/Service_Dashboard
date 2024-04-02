from fastapi import FastAPI, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from user_agents import parse
from geoip2.database import Reader
from datetime import datetime
import pytz


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# SQLite3 데이터베이스 설정
SQLALCHEMY_DATABASE_URL = "sqlite:///./analytics.db"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# GeoIP 데이터베이스 로드
reader = Reader("GeoLite2-City.mmdb")

# 한국 시간대 설정
KST = pytz.timezone("Asia/Seoul")


# 데이터베이스 종속성
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# 모델 정의
class Pageview(Base):
    __tablename__ = "pageviews"
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(KST))
    url = Column(String)  # 사용자가 접속한 URL
    user_location = Column(String)  # 사용자의 지역 정보
    user_agent = Column(String)  # 사용자의 User-Agent 정보
    is_mobile = Column(Integer)  # 모바일 기기 여부
    is_pc = Column(Integer)  # PC 기기 여부


class AnchorClick(Base):
    __tablename__ = "anchor_clicks"
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(KST))
    source_url = Column(String)  # 사용자가 클릭한 링크의 출발 URL
    target_url = Column(String)  # 사용자가 클릭한 링크의 도착 URL
    user_agent = Column(String)  # 사용자의 User-Agent 정보
    is_mobile = Column(Integer)  # 모바일 기기 여부
    is_pc = Column(Integer)  # PC 기기 여부


# 데이터베이스 테이블 생성
Base.metadata.create_all(bind=engine)


# 수집할 데이터의 모델 정의
class PageviewData(BaseModel):
    url: str


class AnchorClickData(BaseModel):
    source_url: str
    target_url: str


@app.post("/collect/pageview")
async def collect_pageview(
    request: Request, data: PageviewData, db: SessionLocal = Depends(get_db)
):
    # User Agent 정보 파싱
    user_agent_string = request.headers.get("User-Agent")
    user_agent = parse(user_agent_string)

    # IP 주소로 지역 정보 파싱
    client_ip = request.client.host
    try:
        response = reader.city(client_ip)
        user_location = f"{response.city.name}, {response.country.name}"
    except:
        user_location = "Unknown"

    # 데이터베이스에 정보 저장
    pageview = Pageview(
        url=data.url,
        user_location=user_location,
        user_agent=user_agent_string,
        is_mobile=int(user_agent.is_mobile),
        is_pc=int(user_agent.is_pc),
    )
    db.add(pageview)
    db.commit()

    return {"status": "success", "message": "Pageview data collected successfully"}


@app.post("/collect/anchor-click")
async def collect_anchor_click(
    request: Request, data: AnchorClickData, db: SessionLocal = Depends(get_db)
):
    user_agent_string = request.headers.get("User-Agent")
    user_agent = parse(user_agent_string)

    anchor_click = AnchorClick(
        source_url=data.source_url,
        target_url=data.target_url,
        user_agent=user_agent_string,
        is_mobile=int(user_agent.is_mobile),
        is_pc=int(user_agent.is_pc),
    )
    db.add(anchor_click)
    db.commit()

    return {"status": "success", "message": "Anchor click data collected successfully"}
