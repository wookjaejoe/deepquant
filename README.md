# DeepQuant

## Build & Deploy
1. 도커 이미지 빌드: `python build.py`
2. 도커 이미지 빌드 중 출력된 실행 커맨드 참고하여 배포 환경에서 `docker run ...` 커맨드 실행

## Export notebook
```
jupyter nbconvert --no-input --to html  analysis.ipynb
```