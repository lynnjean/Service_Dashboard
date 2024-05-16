import secrets
from datetime import datetime, timedelta
import pytz
from geoip2.database import Reader
import logging

KST = pytz.timezone("Asia/Seoul")

# GeoIP 데이터베이스 로드
reader = Reader("GeoLite2-City.mmdb")

logger = logging.getLogger(__name__)

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

# 세션 ID 생성 함수
def generate_session_id():
    return secrets.token_urlsafe(16)  # 16바이트 길이의 무작위 문자열 생성
