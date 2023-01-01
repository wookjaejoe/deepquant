from rest_framework.exceptions import ParseError
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from core.quantpick import QuantPicker


# noinspection PyMethodMayBeStatic
class RecommendAPI(APIView):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.picker = QuantPicker.instance()

    def get(self, request: Request):
        try:
            if "limit" in request.query_params:
                limit = int(request.query_params["limit"])
            else:
                limit = 50
        except:
            raise ParseError(detail="Failed to parse a param: limit")

        body = {
            "updated": self.picker.updated,
            "items": self.picker.head(limit=limit)
        }

        return Response(body)
