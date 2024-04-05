# weniv_analytics
위니브 서비스 전체를 모니터링하는 서비스입니다.

## 목적

- 위니브 서비스의 모니터링을 위한 서비스

## 라이브러리 변동시 업데이트

```
pip freeze > requirements.txt
```

## 동작

* /analytics/anchor-clicks?source_url=example.com&date_start=20240401&date_end=20240430&interval=daily
  * urlA의 20240401부터 20240430까지의 일별 pageviews를 조회합니다.
  * 응답값
    ```json
    {
        "total_pageviews": 171,
        {
            "20240401": {
                "pageviews_by_location": {
                    "Unknown": 171
                },
                "pageviews_by_device": {
                    "mobile": 0,
                    "pc": 171
                },
                "pageviews_by_os": {
                    "Windows": 171
                },
                "pageviews_by_browser": {
                    "Chrome": 171
                }
            },
            "20240402": {
                "pageviews_by_location": {
                    "Unknown": 171
                },
                "pageviews_by_device": {
                    "mobile": 0,
                    "pc": 171
                },
                "pageviews_by_os": {
                    "Windows": 171
                },
                "pageviews_by_browser": {
                    "Chrome": 171
                }
            }
        }
    }
    ```
* analytics/anchor-clicks?url=127.0.0.1&date_start=20240401&date_end=20240430&interval=daily

```
pip install -r requirements.txt
uvicorn main:app --reload
```