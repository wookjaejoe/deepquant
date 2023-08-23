class FnSpaceRequestError(Exception):
    """
    100	권한이 없습니다.
    101	유효한 개인 인증키를 찾을 수 없습니다.
    102	API에 대한 사용권한이 없습니다.
    103	조회가능한 시계열 범위를 벗어났습니다.
    200	코인 및 사용량을 확인하십시오.
    201	하루 최대 사용량을 초과 하였습니다.
    202	최대 사용량을 초과 하였습니다.
    300	필수 값이 누락 되어 있습니다.요청인자를 참고 하십시오
    301	인증키가 유효하지 않습니다.
    302	파일타입 값이 누락 혹은 유효하지 않습니다.
    305	회계 기준이 누락 혹은 유효하지 않습니다.
    306	연간, 분기 값이 누락 혹은 유효하지 않습니다.
    307	결산년월을 확인하십시오.
    308	조회기간을 확인하십시오.
    309	조회 기준을 확인하십시오.
    310	요청종목 값이 누락 혹은 유효하지 않습니다.
    311	최대 요청종목 개수를 초과하였습니다.
    312	요청종목 개수는 10개를 초과할 수 없습니다.
    313	같은 종목을 여러번 요청할 수 없습니다.
    320	요청아이템 값이 누락 혹은 유효하지 않습니다.
    321	최대 요청아이템 개수를 초과하였습니다.
    322	요청아이템 개수는 20개를 초과할 수 없습니다.
    323	같은 아이템을 여러번 요청할 수 없습니다.
    330	해당하는 서비스를 찾을 수 없습니다.
    900	서비스 도중 에러가 발생하였습니다. 관리자에게 문의하세요.
    """

    def __init__(self, body: dict):
        self.full = body
        self.code = int(body["errcd"])
        self.message = body["errmsg"]

    def __str__(self):
        return f"[{self.code}] {self.message}"