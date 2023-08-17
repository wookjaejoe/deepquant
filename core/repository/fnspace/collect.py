import pandas as pd

from core.repository.fnspace import account
from core.repository.fnspace.client import fetch_finance
from core.repository.maria.conn import maria_home

db = maria_home()


def _fetch_and_finance(code: str, items: list, year: int, month: int, sep: bool):
    df = fetch_finance(
        code=code,
        item=items,
        sep=sep,
        year=year,
        month=month
    )
    if df is not None:
        df.to_sql("fnspace_finance", db, if_exists="append", index=False)


def fetch_and_upload_both(code: str, year: int, month: int):
    query = f"""
    select distinct
    consolgb, item from fnspace_finance
    where CODE = '{code}' and FS_YEAR = {year} and FS_MONTH = FS_MONTH
    """
    records = pd.read_sql(query, db)
    items = [acc for acc in account.majors if acc not in records[records["consolgb"] == "C"]["item"].values]
    if len(items) > 0:
        _fetch_and_finance(code=code, items=items, year=year, month=month, sep=False)

    items = [acc for acc in account.majors if acc not in records[records["consolgb"] == "I"]["item"].values]
    if len(items) > 0:
        _fetch_and_finance(code=code, items=items, year=year, month=month, sep=True)
