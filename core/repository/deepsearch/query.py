import json
import logging
from datetime import date

from requests.exceptions import Timeout
from requests import get
from retry import retry

from utils.timeutil import YearQtr
from config import config

_logger = logging.getLogger(__name__)


@retry(exceptions=Timeout, tries=5, delay=1, logger=_logger)
def _query(params):
    response = get(
        'https://api.deepsearch.com/v1/compute',
        params=params,
        headers={'Authorization': config['deepSearchAuth']},
        timeout=30
    )

    assert response.status_code == 200, f'Response code not 200, actual - {response.status_code}'
    content = json.loads(response.content)
    # noinspection DuplicatedCode
    assert content
    assert content["success"] is True
    data = content["data"]
    assert not data["exceptions"]
    subpod = data['pods'][0]['subpods'][0]
    assert subpod['class'] == 'Compiler:CompilationSucceeded'
    assert subpod['content']['data'][0] == 'Compilation succeeded.'
    return content


def query2(stock_code: str, titles: list[str], fromyear: int, toyear: int):
    titles = " ".join(titles)
    params = {"input": f"KRX:{stock_code} {fromyear}-{toyear} 분기 {titles}"}
    return _query(params)


def query(title: str, year: int, quarter: int = None):
    _logger.info(f"Requesting query - title={title}, year={year}, quarter={quarter}")

    today = date.today()
    last_confirmed = YearQtr.last_confirmed(today.year, today.month)
    if quarter:
        assert last_confirmed >= YearQtr(year, quarter)
        params = {'input': f'상장 기업 and {title} {year}년 {quarter}분기'}
    else:
        assert last_confirmed >= YearQtr(year, 4)
        params = {'input': f'상장 기업 and {title} {year}'}

    return _query(params)
