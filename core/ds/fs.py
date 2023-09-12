import logging
from datetime import date

import pandas as pd

from core.ds import GetFinancialStatements
from core.repository import maria_home

_logger = logging.getLogger()

white_accounts = {
    "11:5000": "자산총계",
    "11:2000": "유동자산",
    "11:1100": "현금및현금성자산",
    "11:8900": "자본총계",
    "12:1000": "매출",
    "12:3000": "매출총이익",
    "12:5000": "영업이익",
    "12:8000": "법인세비용차감전계속영업이익",
    "12:8200": "당기순이익",
    "12:9000": "당기순이익",
    "16:1000": "영업활동현금흐름",
    "16:3590": "배당금지급"
}


class FsLoader:

    @property
    def db(self):
        return maria_home("fs")

    @property
    def codes(self):
        with self.db.connect() as con:
            return [row[0] for row in con.execute("show tables")]

    def table(self, code: str):
        acc = ", ".join([f"'{x}'" for x in white_accounts.keys()])
        query = f"""
        select * from `{code}`
        where concat(report_id, ':', account_id) in ({acc}) and date > '2012-12-31'
        """
        return pd.read_sql(query, self.db)

    def all_tables(self):
        result = pd.DataFrame()
        total = len(self.codes)
        num = 0
        for code in self.codes:
            num += 1
            _logger.info(f"[{num}/{total}] {code} ({round(num / total * 100)}%)")
            df = self.table(code)

            if df.empty:
                continue

            df["dlname"] = df[["report_id", "account_id"]].apply(lambda x: white_accounts[":".join(x)], axis=1)
            df = df.pivot_table(
                index=["date", "consolidated"], columns="dlname", values="value",
                aggfunc=lambda x: x.iloc[-1]
            )
            result = pd.concat([result, df])

        return result

    def update(self, code: str, df: pd.DataFrame):
        if df.empty:
            return

        date_in = ",".join([f"'{x}'" for x in df["date"].unique()])
        consolidated = df["consolidated"].unique()
        assert len(consolidated) == 1
        consolidated = int(consolidated[0])
        with self.db.connect() as con:
            query = f"""
            delete from `{code}`
            where consolidated = {consolidated} and date in ({date_in}) 
            """
            con.execute(query)
            return df.to_sql(code, self.db, if_exists="append", index=False)

    def summary(self, code: str):
        query = f"""
        select date, consolidated, count(*) as count
        from `{code}`
        group by date, consolidated;
        """
        return pd.read_sql(query, self.db)


fs_loader = FsLoader()


def update_all(date_from: date, date_to: date):
    num = 0
    for code in fs_loader.codes:
        num += 1
        _logger.info(f"[{num}] {code}")
        for consolidated in [True, False]:
            df = GetFinancialStatements.call_api(
                code=code,
                consolidated=consolidated,
                date_from=date_from,
                date_to=date_to
            )
            if len(df) == 0:
                continue

            df["date"] = pd.to_datetime(df["date"]).dt.date
            df["consolidated"] = consolidated
            assert df["symbol"].nunique() == 1
            code = df["symbol"].iloc[0].split(":")[1]
            df = df.drop(columns=["symbol", "entity_name"])  # 불필요한 칼럼 제거
            df = df.dropna()
            fs_loader.update(code, df)
