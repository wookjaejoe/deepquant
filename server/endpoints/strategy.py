from server.app import app
from core.strategy import recipe, factor_candis
from core.quantpick import QuantPicker

picker = QuantPicker.instance()


@app.get("/strategy/rank")
def _rank():
    return {
        "updated": picker.updated,
        "items": picker.head(limit=100)
    }


@app.get("/strategy/recipe")
def _recipe():
    return recipe


@app.get("/strategy/factors")
def _factors():
    return factor_candis


@app.get("/stock/{code}")
def _stock(code: str):
    return picker.get(code)
