from fastapi import FastAPI, Request, Depends, Header # , Cookie, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from models import Pageview, AnchorClick, WenivSql, PageviewData, AnchorClickData, WenivSqlData, Base
from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import sessionmaker
from user_agents import parse
from datetime import datetime, timedelta
from typing import Optional
from utils import generate_session_id, get_date_range, KST, reader, logger
import requests
from urllib.parse import unquote
import pandas as pd
import httpx
import asyncio
from openai import OpenAI
import re
import json
from dotenv import load_dotenv
import os
import polars as pl
import korea

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

# 데이터베이스 종속성
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# 데이터베이스 테이블 생성
Base.metadata.create_all(bind=engine)

@app.post("/collect/pageview")
async def collect_pageview(
        request: Request, data: PageviewData, db: SessionLocal = Depends(get_db)
        ,user_agent: str = Header(None),session_id: str = Header(None), referer: str = Header(None)
):
    try:
        # User Agent 정보 파싱
        user_agent_string = request.headers.get("User-Agent")
        user_agent = parse(user_agent_string)

        # IP 주소로 지역 정보 파싱
        client_ip = request.headers.get("X-Forwarded-For")
        if client_ip:
            client_ip = client_ip.split(",")[0].strip()
        else:
            client_ip = request.client.host if request.client else None
        try:
            response = reader.city(client_ip)
            user_location = f"{response.city.name}, {response.country.name}"
        except:
            user_location = "Unknown"

        # 세션 ID가 없는 경우 새로 생성
        if session_id is None:
            session_id = generate_session_id()

        # 데이터베이스에 정보 저장
        pageview = Pageview(
            url=data.url,
            referer_url = referer,
            ip_address = client_ip,
            session_id = session_id,
            user_location=user_location,
            user_agent=user_agent_string,
            is_mobile=int(user_agent.is_mobile),
            is_pc=int(user_agent.is_pc),
        )

        if ('127.0.0.1' not in data.url) and ('localhost' not in data.url) and ('webpagetest' not in user_agent.browser[0]) and ('bot' not in user_agent_string.lower()) and ('yeti' not in user_agent_string.lower()) and ('headlesschrome' not in user_agent_string.lower()):
            db.add(pageview)
            db.commit()

    except Exception as e:
        logger.log(logging.DEBUG, f"Error: {e}")

    return {"status": "success", "message": "Pageview data collected successfully", "session_id":session_id, "referer_url":referer}

@app.get("/analytics/pageviews") # 접속횟수, 날짜 필터링
async def get_pageviews_usercount(
        url: str,
        date_start: str,
        date_end: str,
        interval: str = "daily",
        db: SessionLocal = Depends(get_db),
):
    start_date, end_date = get_date_range(date_start, date_end, interval)

    pageviews = (
        db.query(Pageview)
        .filter(
            Pageview.url.like(f"%{url}%"),
            Pageview.timestamp >= start_date,
            Pageview.timestamp <= end_date.replace(hour=23, minute=59, second=59),
        )
        .all()
    )

    processed_data = {
        "total_pageviews": {},
        "num": {},
        "pageviews_by_device":{},
        "pageviews_by_os":{},
        "pageviews_by_location": {},
        "pageviews_by_browser": {},
        "daily_time":{},
        f"is_{interval}_week":{}
    }

    all_dates = pd.date_range(start=start_date, end=end_date, freq='D')
    all_dates_df = pd.DataFrame(all_dates, columns=['date'])    
    all_dates_df['date'] = all_dates_df['date'].dt.strftime('%Y%m%d')

    if pageviews:
        pageviews_df = pd.DataFrame(
            [
                {
                    "timestamp": p.timestamp, 
                    "session_id": p.session_id,
                    "is_mobile":p.is_mobile,
                    "is_pc":p.is_pc,
                    "os": parse(p.user_agent).os.family,
                    "location":p.user_location,
                    "browser": parse(p.user_agent).browser.family,
                } for p in pageviews],
        )

        processed_data['total_pageviews'] = len(pageviews_df['timestamp'])
        pageviews_df['date']=pageviews_df['timestamp'].dt.strftime('%Y%m%d')
        merged_df = all_dates_df.merge(pageviews_df, on='date', how='left')

        if interval == "daily": 
            pageviews = merged_df.groupby('date').agg(session_count=('session_id', 'count')).reset_index()
        elif interval == "weekly" or interval == "monthly":
            merged_df['date'] = pd.to_datetime(merged_df['date'])
            merged_df = merged_df.set_index('date')
            if interval == "weekly":
                pageviews = merged_df.resample('W').agg(session_count=('session_id', 'count')).reset_index()
                pageviews['date'] = (pageviews['date'] + pd.DateOffset(days=-6)).dt.strftime('%Y%m%d') + '-' + pageviews['date'].dt.strftime('%Y%m%d')
            elif interval == "monthly":
                pageviews = merged_df.resample('M').agg(session_count=('session_id', 'count')).reset_index()
                pageviews['date'] = pageviews['date'].dt.strftime('%Y%m') 
        
        pc = pageviews_df[['is_pc']].sum()
        mobile = pageviews_df[['is_mobile']].sum()

        os = pageviews_df['os'].value_counts().sort_values(ascending=False).to_dict()
        browser = pageviews_df['browser'].value_counts().sort_values(ascending=False).to_dict()

        t = pd.Series(0,index=list(range(0,24)))
        times = pageviews_df['timestamp'].dt.hour.value_counts().sort_index()
        co_time = pd.concat([t, times]).to_dict()

        w = pd.Series(0,index=["Monday","Thursday","Wednesday","Tuesday","Friday","Saturday","Sunday"])
        weeks = pageviews_df['timestamp'].dt.strftime("%A").value_counts().sort_index()
        co_week = pd.concat([w, weeks]).to_dict()

        pageviews_df['country'] = pageviews_df['location'].str.split(', ').str[1]
        country = pageviews_df['country'].value_counts().sort_values(ascending=False).to_dict()

        korea_df=pageviews_df[pageviews_df['country']=='South Korea']
        korea_df['city'] = korea_df['location'].str.split(', ').str[0]
        city = korea_df['city'].value_counts().reset_index()
        
        def find_location(city):
            for key, value in korea.location_dict.items():
                if city in value:
                    return key
            return 'Unknown'

        city['region'] = city['city'].apply(find_location)
        city = city['region'].value_counts().sort_values(ascending=False).to_dict()

        processed_data['pageviews_by_device'] = {'pc':int(pc),'mobile':int(mobile)}
        processed_data['pageviews_by_os'] = {os: count for os, count in os.items()}
        processed_data['pageviews_by_location']['country'] = country
        processed_data['pageviews_by_location']['city'] = city
        processed_data['pageviews_by_browser'] = {browser: count for browser, count in browser.items()}
        processed_data['daily_time'] = {time: count for time, count in co_time.items()}
        processed_data[f'is_{interval}_week'] = {week: count for week, count in co_week.items()}
    else:
        processed_data['total_pageviews'] = 0
        pageviews = all_dates_df.copy()
        pageviews['session_count'] = 0
    
    processed_data['num'] = {row['date']: row['session_count'] for _, row in pageviews.iterrows()}
    processed_data['num']['min'] = int(pageviews['session_count'].min())
    processed_data['num']['max'] = int(pageviews['session_count'].max())
    processed_data['num']['avg'] = int(pageviews['session_count'].mean())

    return processed_data

@app.get("/analytics/pageviews/usercount") # 접속자수, 날짜 필터링
async def get_pageviews_usercount(
        url: str,
        date_start: str,
        date_end: str,
        interval: str = "daily",
        db: SessionLocal = Depends(get_db),
):
    start_date, end_date = get_date_range(date_start, date_end, interval)

    pageviews = (
        db.query(Pageview)
        .filter(
            Pageview.url.like(f"%{url}%"),
            Pageview.timestamp >= start_date,
            Pageview.timestamp <= end_date.replace(hour=23, minute=59, second=59),
        )
        .all()
    )

    processed_data = {
        "total_pageviews": {},
        "num": {},
        "pageviews_by_device":{},
        "pageviews_by_os":{},
        "pageviews_by_location": {},
        "pageviews_by_browser": {},
        "daily_time":{},
        f"is_{interval}_week":{}
    }

    all_dates = pd.date_range(start=start_date, end=end_date, freq='D')
    all_dates_df = pd.DataFrame(all_dates, columns=['date'])    
    all_dates_df['date'] = all_dates_df['date'].dt.strftime('%Y%m%d')

    if pageviews:
        pageviews_df = pd.DataFrame(
            [
                {
                    "timestamp": p.timestamp, 
                    "session_id": p.session_id,
                    "is_mobile":p.is_mobile,
                    "is_pc":p.is_pc,
                    "os": parse(p.user_agent).os.family,
                    "location":p.user_location,
                    "browser": parse(p.user_agent).browser.family,
                } for p in pageviews],
        )

        processed_data['total_pageviews'] = len(pageviews_df["session_id"].unique())
        pageviews_df['date']=pageviews_df['timestamp'].dt.strftime('%Y%m%d')
        merged_df = all_dates_df.merge(pageviews_df, on='date', how='left')

        if interval == "daily": 
            pageviews = merged_df.groupby('date').agg(session_count=('session_id', 'nunique')).reset_index()
        elif interval == "weekly" or interval == "monthly":
            merged_df['date'] = pd.to_datetime(merged_df['date'])
            merged_df = merged_df.set_index('date')
            if interval == "weekly":
                pageviews = merged_df.resample('W').agg(session_count=('session_id', 'nunique')).reset_index()
                pageviews['date'] = (pageviews['date'] + pd.DateOffset(days=-6)).dt.strftime('%Y%m%d') + '-' + pageviews['date'].dt.strftime('%Y%m%d')
            elif interval == "monthly":
                pageviews = merged_df.resample('M').agg(session_count=('session_id', 'nunique')).reset_index()
                pageviews['date'] = pageviews['date'].dt.strftime('%Y%m') 
        
        session_df = pageviews_df.drop_duplicates(subset=['session_id'])
        pc = session_df[['is_pc']].sum()
        mobile = session_df[['is_mobile']].sum()

        os = session_df['os'].value_counts().sort_values(ascending=False).to_dict()
        browser = session_df['browser'].value_counts().sort_values(ascending=False).to_dict()

        t = pd.Series(0,index=list(range(0,24)))
        times = session_df['timestamp'].dt.hour.value_counts().sort_index()
        co_time = pd.concat([t, times]).to_dict()

        w = pd.Series(0,index=["Monday","Thursday","Wednesday","Tuesday","Friday","Saturday","Sunday"])
        weeks = session_df['timestamp'].dt.strftime("%A").value_counts().sort_index()
        co_week = pd.concat([w, weeks]).to_dict()

        session_df['country'] = session_df['location'].str.split(', ').str[1]
        country = session_df['country'].value_counts().sort_values(ascending=False).to_dict()

        korea_df=session_df[session_df['country']=='South Korea']
        korea_df['city'] = korea_df['location'].str.split(', ').str[0]
        city = korea_df['city'].value_counts().reset_index()
        
        def find_location(city):
            for key, value in korea.location_dict.items():
                if city in value:
                    return key
            return 'Unknown'

        city['region'] = city['city'].apply(find_location)
        city = city['region'].value_counts().sort_values(ascending=False).to_dict()

        processed_data['pageviews_by_device'] = {'pc':int(pc),'mobile':int(mobile)}
        processed_data['pageviews_by_os'] = {os: count for os, count in os.items()}
        processed_data['pageviews_by_location']['country'] = country
        processed_data['pageviews_by_location']['city'] = city
        processed_data['pageviews_by_browser'] = {browser: count for browser, count in browser.items()}
        processed_data['daily_time'] = {time: count for time, count in co_time.items()}
        processed_data[f'is_{interval}_week'] = {week: count for week, count in co_week.items()}
    else:
        processed_data['total_pageviews'] = 0
        pageviews = all_dates_df.copy()
        pageviews['session_count'] = 0
    
    processed_data['num'] = {row['date']: row['session_count'] for _, row in pageviews.iterrows()}
    processed_data['num']['min'] = int(pageviews['session_count'].min())
    processed_data['num']['max'] = int(pageviews['session_count'].max())
    processed_data['num']['avg'] = int(pageviews['session_count'].mean())

    return processed_data

@app.get('/analytics/pageviews/active_users') # 활성화 유저 수(dau, wau, mau)
async def active_users(
        url: str,
        db: SessionLocal = Depends(get_db),
):
    # 오늘 날짜 구하기
    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

    daily_pageviews = (
        db.query(func.count(Pageview.session_id.distinct()))
        .filter(
            Pageview.url.like(f"%{url}%"),
            Pageview.timestamp >= today,
            Pageview.timestamp < today + timedelta(days=1),
        )
        .scalar()
    )
    weekly_pageviews = (
        db.query(func.count(Pageview.session_id.distinct()))
        .filter(
            Pageview.url.like(f"%{url}%"),
            Pageview.timestamp >= today - timedelta(days=6),
            Pageview.timestamp < today + timedelta(days=1),
        )
        .scalar()
    )
    monthly_pageviews = (
        db.query(func.count(Pageview.session_id.distinct()))
        .filter(
            Pageview.url.like(f"%{url}%"),
            Pageview.timestamp >= today - timedelta(days=30),
            Pageview.timestamp < today + timedelta(days=1),
        )
        .scalar()
    )

    # 데이터 가공
    processed_data = {
        "dau": {},
        "wau": {},
        "mau": {},
    }

    if monthly_pageviews:
        today_str = today.strftime("%Y%m%d")
        processed_data["dau"][today_str] = daily_pageviews
        processed_data["dau"]['일주일평균(일)'] = weekly_pageviews/7
        processed_data["dau"]['월평균(일)'] = monthly_pageviews/30
        processed_data["wau"][f'{(today - timedelta(days=6)).strftime("%Y%m%d")} ~ {today_str}'] = weekly_pageviews
        processed_data["mau"][f'{(today - timedelta(days=29)).strftime("%Y%m%d")} ~ {today_str}'] = monthly_pageviews

    return processed_data

@app.get('/analytics/pageviews/top5') # mau 기준 서비스 top5
def pageview_top5(        
    interval: str = "daily",
):
    service_list = [
        'books.weniv',
        'weniv.link',
        'world.weniv',
        'sql.weniv',
        'notebook.weniv'
    ]

    service = {}
    for name in service_list:
        response = requests.get(f'https://analytics.weniv.co.kr/analytics/pageviews/active_users?url={name}')

        if response.status_code == 200:
            # JSON 응답 데이터 파싱
            data = response.json()
            if interval=='daily' and 'dau' in data and data['dau']:
                service[name]=next(iter(data['dau'].values()))
            elif interval=='weekly' and 'wau' in data and data['wau']:
                service[name]=next(iter(data['wau'].values()))
            elif interval=='monthly' and 'mau' in data and data['mau']:
                service[name]=next(iter(data['mau'].values()))

    # 값 기준으로 정렬
    service_sort = sorted(service.items(), key=lambda x: x[1], reverse=True)

    # 상위 5개 항목 추출
    top5 = dict(service_sort[:5])

    return top5

@app.post("/collect/anchor-click")
async def collect_anchor_click(
        request: Request, data: AnchorClickData, db: SessionLocal = Depends(get_db)
        ,user_agent: str = Header(None),session_id: str = Header(None,alias="Session-Id")
):
    user_agent_string = request.headers.get("User-Agent")
    user_agent = parse(user_agent_string)

    # IP 주소로 지역 정보 파싱
    client_ip = request.headers.get("X-Forwarded-For")
    if client_ip:
        client_ip = client_ip.split(",")[0].strip()
    else:
        client_ip = request.client.host if request.client else None
    try:
        response = reader.city(client_ip)
        user_location = f"{response.city.name}, {response.country.name}"
    except:
        user_location = "Unknown"

    session_id = request.headers.get('Session-Id')
    
    # 세션 ID가 없는 경우 새로 생성
    if session_id is None:
        session_id = generate_session_id()  

    anchor_click = AnchorClick(
        source_url=data.source_url,
        target_url=data.target_url,
        ip_address = client_ip,
        session_id = session_id,
        user_agent=user_agent_string,
        is_mobile=int(user_agent.is_mobile),
        is_pc=int(user_agent.is_pc),
        type = data.type
    )

    excluded_sources = ['127.0.0.1', 'localhost']
    excluded_agents = ['bot', 'yeti','headlesschrome']

    if all(source not in data.source_url for source in excluded_sources) and \
    all(agent not in user_agent_string.lower() for agent in excluded_agents) and ('webpagetest' not in user_agent.browser[0]):
        db.add(anchor_click)
        db.commit()

    return {"status": "success", "message": "Anchor click data collected successfully"}

@app.get("/analytics/anchor-clicks") # 다른 컨텐츠 이동 횟수
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
            AnchorClick.timestamp >= start_date, AnchorClick.timestamp <= end_date.replace(hour=23, minute=59, second=59),
        )

    anchor_clicks = query.all()

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

        if interval == "daily":
            date_key = current_date.strftime("%Y%m%d")
        elif interval == "weekly":
            date_key = f"{current_date.strftime('%Y%m%d')}-{(next_date - timedelta(days=1)).strftime('%Y%m%d')}"
        elif interval == "monthly":
            date_key = current_date.strftime("%Y%m")

        # 기본 구조 생성
        processed_data["data"][date_key] = {
            "clicks_by_target_url": {},
            "clicks_by_device": {
                "mobile": 0,
                "pc": 0,
            },
            "clicks_by_os": {},
            "clicks_by_browser": {},
        }

        # 데이터가 있는 경우에만 처리
        if filtered_clicks:
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

@app.post("/collect/sql")
async def collect_sql(
        request: Request, data: WenivSqlData, db: SessionLocal = Depends(get_db)
        ,user_agent: str = Header(None),session_id: str = Header(None,alias="Session-Id")
):
    try:
        user_agent_string = request.headers.get("User-Agent")
        user_agent = parse(user_agent_string)

        # IP 주소로 지역 정보 파싱
        client_ip = request.headers.get("X-Forwarded-For")
        if client_ip:
            client_ip = client_ip.split(",")[0].strip()
        else:
            client_ip = request.client.host if request.client else None
        try:
            response = reader.city(client_ip)
            user_location = f"{response.city.name}, {response.country.name}"
        except:
            user_location = "Unknown"

        session_id = request.headers.get('Session-Id')
        
        # 세션 ID가 없는 경우 새로 생성
        if session_id is None:
            session_id = generate_session_id()  

        sql_data = WenivSql(
            contents=data.contents,
            ip_address = client_ip,
            session_id = session_id,
            user_agent=user_agent_string,
            is_mobile=int(user_agent.is_mobile),
            is_pc=int(user_agent.is_pc),
        )

        db.add(sql_data)
        db.commit()
        
    except Exception as e:
        logger.log(logging.DEBUG, f"Error: {e}")

    return {"status": "success", "message": "sql data collected successfully"}

@app.get("/analytics/wenivbooks/url") # 조회 수 높은 페이지
async def get_urlcount(
        date_start: str,
        date_end: str,
        interval: str = "daily",
        book: str="",
        db: SessionLocal = Depends(get_db),
):
    start_date, end_date = get_date_range(date_start, date_end, interval)

    wenivbooks_pageviews = (
        db.query(Pageview.url, func.count(Pageview.url))
        .filter(
            Pageview.url.like(f"%books.weniv%"),
            Pageview.url.like(f"%{book}%"),
            ~Pageview.url.like(f"%keyword%"),
            Pageview.timestamp >= start_date,
            Pageview.timestamp <= end_date.replace(hour=23, minute=59, second=59),
        )
        .group_by(Pageview.url)
        .all()
    )

    book_list=['sql','github','html-css','basecamp-html-css','basecamp-javascript',
    'basecamp-network','javascript','python','wenivworld','wenivworld-teacher', 'basecamp-sql', 'figma-serenade']
        
    df = pd.DataFrame(wenivbooks_pageviews, columns=['url', 'count'])
    try:
        df['url_split'] = df['url'].apply(lambda x: x.split('/')[3] if len(x.split('/')) > 3 else None)
    except IndexError:
        pass    
    df = df[df['url_split'].isin(book_list)]
    df.drop(columns=['url_split'], inplace=True)
    df = df.sort_values(by='count', ascending=False)
    result = df[:20].to_dict('records')

    return result

@app.get("/analytics/wenivbooks/tech") # 조회 수 높은 교안
async def get_techcount(
        date_start: str,
        date_end: str,
        interval: str = "daily",
        db: SessionLocal = Depends(get_db),
):
    start_date, end_date = get_date_range(date_start, date_end, interval)

    wenivbooks_pageviews = (
        db.query(Pageview.url, func.count(Pageview.url))
        .filter(
            Pageview.url.like(f"%books.weniv%"),
            ~Pageview.url.like(f"%keyword%"),
            Pageview.timestamp >= start_date,
            Pageview.timestamp <= end_date.replace(hour=23, minute=59, second=59),
        )
        .group_by(Pageview.url)
        .all()
    )

    book_list=['sql','github','html-css','basecamp-html-css','basecamp-javascript',
    'basecamp-network','javascript','python','wenivworld','wenivworld-teacher', 'basecamp-sql', 'figma-serenade']
        
    df = pd.DataFrame(wenivbooks_pageviews, columns=['url', 'count'])
    try:
        df['url_split'] = df['url'].apply(lambda x: x.split('/')[3] if len(x.split('/')) > 3 else None)
    except IndexError:
        pass    

    df = df[df['url_split'].isin(book_list)]
    df['url'] = df['url'].apply(lambda x: unquote(x, 'utf-8') if '%' in x else x)
    grouped_df = df.groupby('url_split')['count'].sum().reset_index()
    result = grouped_df.set_index('url_split').T.to_dict('records')

    return result

@app.get("/analytics/wenivbooks/keyword") # 검색 키워드
async def get_keyword(
        date_start: str,
        date_end: str,
        interval: str = "daily",
        db: SessionLocal = Depends(get_db),
):
    start_date, end_date = get_date_range(date_start, date_end, interval)

    keyword_pageviews = (
        db.query(Pageview.url, func.count(Pageview.url))
        .filter(
            Pageview.url.like(f"%books.weniv%"),
            Pageview.url.like(f"%search?keyword%"),
            Pageview.timestamp >= start_date,
            Pageview.timestamp <= end_date.replace(hour=23, minute=59, second=59),
        )
        .group_by(Pageview.url)
        .all()
    )

    keyword_dict={}

    for url, count in keyword_pageviews:
        keyword = url.split('=')[1]
        if '%' in keyword:
            keyword = unquote(keyword, 'utf-8')
        if keyword in keyword_dict:
            keyword_dict[keyword] += count
        else:
            keyword_dict[keyword] = count
    
    result = {keyword: count for keyword, count in keyword_dict.items()}

    return result

@app.post("/analytics/ai")
async def analytics_ai(
        request: Request, db: SessionLocal = Depends(get_db)
):
    try:
        body = await request.json()
        question = body.get("question")

        # .env 파일의 환경 변수를 로드합니다.
        load_dotenv()

        OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
        client = OpenAI(api_key=OPENAI_API_KEY)

        completion = client.chat.completions.create(
            model = 'gpt-3.5-turbo',
            messages = [
                {"role":"user","content":question}
            ]
        )

        pattern = r'```sql(.*?)```'
        match = re.search(pattern, completion.choices[0].message.content, re.DOTALL)

        if match:
            sql_text = match.group(1).strip()
            query=text(sql_text)
            result = db.execute(query)
            users = result.mappings().all()
            return {'gpt':completion.choices[0].message.content,'sql': sql_text,'result':users}
        else:
            return {"result":"다시 요청 부탁드립니다.",'gpt':completion.choices[0].message.content}
    except Exception as e:
        return {"error": str(e)}

@app.post("/analytics/sql")
async def analytics_sql(
        request: Request, db: SessionLocal = Depends(get_db)
):
    try:
        body = await request.json()
        question = body.get("question")

        query=text(question)
        result = db.execute(query)
        users = result.mappings().all()
        file = pd.DataFrame(users)
        file.to_csv('sql_result.csv', index=False)
        
        return {'result':users}

    except Exception as e:
        return {"error": str(e)}

@app.get("/analytics/sql/download")
async def download_csv():
    csv_file_path = 'sql_result.csv'
    if not os.path.exists(csv_file_path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(csv_file_path, filename='sql_result.csv')

# health check
@app.get("/health")
def health_check():
    return {"status": "ok"}