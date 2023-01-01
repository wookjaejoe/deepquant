from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from core.quantpick import QuantPicker


class HealthAPI(APIView):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.picker = QuantPicker.instance()

    def get(self, request: Request):
        return Response({
            "stockrtConnected": self.picker.websocket.open
        })
