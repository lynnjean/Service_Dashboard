from fastapi import FastAPI, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from user_agents import parse
from geoip2.database import Reader
from datetime import datetime, timedelta
import pytz
from typing import Optional

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
    await db.add(pageview)
    await db.commit()

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
    await db.add(anchor_click)
    await db.commit()

    return {"status": "success", "message": "Anchor click data collected successfully"}


def get_date_range(date_start: str, date_end: str, interval: str):
    start_date = datetime.strptime(date_start, "%Y%m%d")
    end_date = datetime.strptime(date_end, "%Y%m%d")

    if interval == "daily":
        return start_date, end_date
    elif interval == "weekly":
        start_date = start_date - timedelta(days=start_date.weekday())
        end_date = end_date - timedelta(days=end_date.weekday()) + timedelta(days=6)
    elif interval == "monthly":
        start_date = start_date.replace(day=1)
        end_date = end_date.replace(day=1) + timedelta(days=32)
        end_date = end_date.replace(day=1) - timedelta(days=1)

    return start_date, end_date


@app.get("/analytics/pageviews")
async def get_pageviews(
        url: str,
        date_start: str,
        date_end: str,
        interval: str = "daily",
        db: SessionLocal = Depends(get_db),
):
    start_date, end_date = get_date_range(date_start, date_end, interval)

    pageviews = await (
        db.query(Pageview)
        .filter(
            Pageview.url.like(f"%{url}%"),
            Pageview.timestamp >= start_date,
            Pageview.timestamp <= end_date,
        )
        .all()
    )

    # 데이터 가공
    processed_data = {
        "total_pageviews": len(pageviews),
        "data": {},
    }

    # 일일, 주간, 월간별 데이터 계산
    current_date = start_date
    while current_date <= end_date:
        if interval == "daily":
            next_date = current_date + timedelta(days=1)
        elif interval == "weekly":
            next_date = current_date + timedelta(days=7)
        elif interval == "monthly":
            next_date = current_date.replace(day=28) + timedelta(days=4)
            next_date = next_date.replace(day=1)

        filtered_pageviews = [
            p for p in pageviews if current_date <= p.timestamp < next_date
        ]

        # 데이터가 있는 경우에만 처리
        if filtered_pageviews:

            if interval == "daily":
                date_key = current_date.strftime("%Y%m%d")
            elif interval == "weekly":
                date_key = f"{current_date.strftime('%Y%m%d')}-{(next_date - timedelta(days=1)).strftime('%Y%m%d')}"
            elif interval == "monthly":
                date_key = current_date.strftime("%Y%m")

            processed_data["data"][date_key] = {
                "pageviews_by_location": {},
                "pageviews_by_device": {
                    "mobile": sum(p.is_mobile for p in filtered_pageviews),
                    "pc": sum(p.is_pc for p in filtered_pageviews),
                },
                "pageviews_by_os": {},
                "pageviews_by_browser": {},
            }

            # 지역별 페이지뷰 수 계산
            for pageview in filtered_pageviews:
                if (
                        pageview.user_location
                        not in processed_data["data"][date_key]["pageviews_by_location"]
                ):
                    processed_data["data"][date_key]["pageviews_by_location"][
                        pageview.user_location
                    ] = 0
                processed_data["data"][date_key]["pageviews_by_location"][
                    pageview.user_location
                ] += 1

            # OS별 페이지뷰 수 계산
            for pageview in filtered_pageviews:
                user_agent = parse(pageview.user_agent)
                os_family = user_agent.os.family
                if os_family not in processed_data["data"][date_key]["pageviews_by_os"]:
                    processed_data["data"][date_key]["pageviews_by_os"][os_family] = 0
                processed_data["data"][date_key]["pageviews_by_os"][os_family] += 1

            # 브라우저별 페이지뷰 수 계산
            for pageview in filtered_pageviews:
                user_agent = parse(pageview.user_agent)
                browser_family = user_agent.browser.family
                if (
                        browser_family
                        not in processed_data["data"][date_key]["pageviews_by_browser"]
                ):
                    processed_data["data"][date_key]["pageviews_by_browser"][
                        browser_family
                    ] = 0
                processed_data["data"][date_key]["pageviews_by_browser"][
                    browser_family
                ] += 1

        current_date = next_date

    return processed_data


@app.get("/analytics/anchor-clicks")
async def get_anchor_clicks(
        source_url: str,
        target_url: Optional[str] = None,
        date_start: str = "",
        date_end: str = "",
        interval: str = "daily",
        db: SessionLocal = Depends(get_db),
):
    query = db.query(AnchorClick).filter(AnchorClick.source_url.like(f"%{source_url}%"))

    if target_url:
        query = query.filter(AnchorClick.target_url.like(f"%{target_url}%"))

    if date_start and date_end:
        start_date, end_date = get_date_range(date_start, date_end, interval)
        query = query.filter(
            AnchorClick.timestamp >= start_date, AnchorClick.timestamp <= end_date
        )

    anchor_clicks = await query.all()

    # 데이터 가공
    processed_data = {
        "total_clicks": len(anchor_clicks),
        "data": {},
    }

    # 일일, 주간, 월간별 데이터 계산
    current_date = start_date
    while current_date <= end_date:
        if interval == "daily":
            next_date = current_date + timedelta(days=1)
        elif interval == "weekly":
            next_date = current_date + timedelta(days=7)
        elif interval == "monthly":
            next_date = current_date.replace(day=28) + timedelta(days=4)
            next_date = next_date.replace(day=1)

        filtered_clicks = [
            c for c in anchor_clicks if current_date <= c.timestamp < next_date
        ]

        # 데이터가 있는 경우에만 처리
        if filtered_clicks:
            if interval == "daily":
                date_key = current_date.strftime("%Y%m%d")
            elif interval == "weekly":
                date_key = f"{current_date.strftime('%Y%m%d')}-{(next_date - timedelta(days=1)).strftime('%Y%m%d')}"
            elif interval == "monthly":
                date_key = current_date.strftime("%Y%m")

            processed_data["data"][date_key] = {
                "clicks_by_target_url": {},
                "clicks_by_device": {
                    "mobile": sum(c.is_mobile for c in filtered_clicks),
                    "pc": sum(c.is_pc for c in filtered_clicks),
                },
                "clicks_by_os": {},
                "clicks_by_browser": {},
            }

            # 도착 URL별 클릭 수 계산
            for click in filtered_clicks:
                if (
                        click.target_url
                        not in processed_data["data"][date_key]["clicks_by_target_url"]
                ):
                    processed_data["data"][date_key]["clicks_by_target_url"][
                        click.target_url
                    ] = 0
                processed_data["data"][date_key]["clicks_by_target_url"][
                    click.target_url
                ] += 1

            # OS별 클릭 수 계산
            for click in filtered_clicks:
                user_agent = parse(click.user_agent)
                os_family = user_agent.os.family
                if os_family not in processed_data["data"][date_key]["clicks_by_os"]:
                    processed_data["data"][date_key]["clicks_by_os"][os_family] = 0
                processed_data["data"][date_key]["clicks_by_os"][os_family] += 1

            # 브라우저별 클릭 수 계산
            for click in filtered_clicks:
                user_agent = parse(click.user_agent)
                browser_family = user_agent.browser.family
                if (
                        browser_family
                        not in processed_data["data"][date_key]["clicks_by_browser"]
                ):
                    processed_data["data"][date_key]["clicks_by_browser"][
                        browser_family
                    ] = 0
                processed_data["data"][date_key]["clicks_by_browser"][
                    browser_family
                ] += 1

        current_date = next_date

    return processed_data


# health check
@app.get("/health")
def health_check():
    return {"status": "ok"}
