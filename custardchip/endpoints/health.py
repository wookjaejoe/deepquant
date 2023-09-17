from custardchip.app import app


@app.get("/")
def health():
    return {"status": "ok"}
