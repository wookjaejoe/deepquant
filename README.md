# DeepQuant

재무지표 기반 퀀트 투자 도구

## Features

- 재무 데이터 수집
- 가격지표, 재무비율, 투자지표 등 계산
- 수익률 상관성 분석 통한 팩터 검증
- 다변수 팩터 기반 퀀트투자 전략 수립 및 종목 선정 도구 개발

## DevOps

### Handling fnlttSinglAcntAll

1. OpenDart API > fnlttSinglAcntAll 이용하여 MongoDB에 요청과 응답 원본 저장
2. MongoDB > fnlttSinglAcntAll 컬렉션의 다큐먼트 하나씩 읽어서 MariaDB에 핸들링하기 편한 형태로 1차 가공하여 삽입
3. MariaDB > fnlttSinglAcntAll 테이블에서 하나씩 읽어서 2차 가공해서 사용

### Build & Deploy

1. 도커 이미지 빌드: `python build.py`
2. 도커 이미지 빌드 중 출력된 실행 커맨드 참고하여 배포 환경에서 `docker run ...` 커맨드 실행

### Export notebook

```
jupyter nbconvert --no-input --to html  analysis.ipynb
```