from dataleft.app import app

from core.repository import get_stocks

stocks = get_stocks()


@app.get("/stocks")
def _stocks():
    return stocks
