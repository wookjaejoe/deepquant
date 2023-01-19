import jsons
from rest_framework.exceptions import ParseError
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from core.quantpick import QuantPicker
from core.strategy import recipe, factor_candis

picker = QuantPicker.instance()


# noinspection PyMethodMayBeStatic
class RecommendAPI(APIView):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get(self, request: Request):
        try:
            if "limit" in request.query_params:
                limit = int(request.query_params["limit"])
            else:
                limit = 50
        except:
            raise ParseError(detail="Failed to parse a param: limit")

        body = {
            "updated": picker.updated,
            "items": picker.head(limit=limit)
        }

        return Response(body)


class RecipeAPI(APIView):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get(request: Request):
        return Response(recipe)


class RecipeDistributionAPI(APIView):

    @staticmethod
    def get(request: Request, title: str):
        return Response(picker.distribution(title))


class FactorsAPI(APIView):

    @staticmethod
    def get(request: Request):
        return Response(jsons.dump(factor_candis))


class StockAPI(APIView):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get(request: Request, code: str):
        return Response(picker.get(code))
