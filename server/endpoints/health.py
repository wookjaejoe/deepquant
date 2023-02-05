from core.quantpick import QuantPicker
from server.app import app

picker = QuantPicker.instance()


@app.get("/")
def strategy():
    return {
        "stockrtConnected": picker.websocket.open
    }
