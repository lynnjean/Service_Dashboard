# weniv_analytics
위니브 서비스 전체를 모니터링하는 서비스입니다.

## 목적

- 위니브 서비스의 모니터링을 위한 서비스

## 라이브러리 변동시 업데이트

```
pip freeze > requirements.txt
```

## 동작

```
pip install -r requirements.txt
uvicorn main:app --reload
```