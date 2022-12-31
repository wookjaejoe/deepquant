import json
import logging
from datetime import date

from requests import get

from base.timeutil import YearQuarter
from config import config

_logger = logging.getLogger(__file__)


def query(title: str, year: int, quarter: int = None):
    _logger.info(f"Requesting query - title={title}, year={year}, quarter={quarter}")

    today = date.today()
    last_confirmed = YearQuarter.last_confirmed(today.year, today.month)
    if quarter:
        assert last_confirmed >= YearQuarter(year, quarter)
        params = {'input': f'상장 기업 and {title} {year}년 {quarter}분기'}
    else:
        assert last_confirmed >= YearQuarter(year, 4)
        params = {'input': f'상장 기업 and {title} {year}'}

    response = get(
        'https://api.deepsearch.com/v1/compute',
        params=params,
        headers={'Authorization': config['deepSearchAuth']}
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


