from datetime import date

from core import ds


def call(code: str) -> dict:
    assert len(code) == 6
    return ds.call(
        "GetAvailableFinancialStatements",
        f"KRX:{code}",
        date_from="1990-01-01",
        date_to=date.today()
    )
