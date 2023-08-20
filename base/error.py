class HttpRequestNotOk(Exception):
    def __init__(self, code: int):
        self.code = code

    def __str__(self):
        return f"code: {self.code}"
