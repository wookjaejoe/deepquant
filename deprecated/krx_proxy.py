import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import Response

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def run(request: Request, call_next):
    host = "data.krx.co.kr"
    url = f"http://{host}{request.url.path}"

    # header > host 값 변경
    headers = dict(request.headers)
    headers[[k for k in headers.keys() if k.lower() == "host"][0]] = host
    async with httpx.AsyncClient() as client:
        content = await request.body()
        response = await client.request(
            method=request.method,
            url=url,
            headers=headers,
            content=content,
            params=None,
            json=None
        )

        return Response(
            content=response.content,
            status_code=response.status_code,
        )
