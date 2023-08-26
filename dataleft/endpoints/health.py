from dataleft.app import app


@app.get("/")
def health():
    return {"status": "ok"}
