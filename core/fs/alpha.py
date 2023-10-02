import logging
from datetime import date

import pandas as pd
from sqlalchemy import text

from core.ds import GetFinancialStatements
from core.repository import maria_home
from utils import pdutil
from multiprocessing.pool import ThreadPool

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


class FsAlpha:
    @property
    def con(self):
        return maria_home("fs")

    @property
    def codes(self):
        with self.con.connect() as con:
            return [row[0] for row in con.execute(text("show tables"))]

    def table(self, code: str, con=None):
        """
        :param code: 종목코드 6자리 문자열
        :param con: 커넥션 객체, 미 입력시 새로 연결
        """
        if not con:
            con = self.con

        acc = ", ".join([f"'{x}'" for x in white_accounts.keys()])
        query = f"""
        select * from `{code}`
        where concat(report_id, ':', account_id) in ({acc})
        """
        return pd.read_sql(query, con)

    def transform(self):
        """
        두번째 형식으로 변형
        """
        con = self.con

        def transform_one(code: str):
            df = self.table(code, con)
            if df.empty:
                return

            df["title"] = df[["report_id", "account_id"]].apply(lambda x: white_accounts[":".join(x)], axis=1)
            df = df.pivot_table(
                index=["date", "type_id", "consolidated"], columns="title", values="value",
                aggfunc=lambda x: x.iloc[-1]
            )
            df = df.reset_index()
            df["year"] = df["date"].apply(lambda x: x.year)
            df["month"] = df["date"].apply(lambda x: x.month)
            df["qtr"] = df["type_id"].replace({["F", "B", "T", "K"][q - 1]: q for q in [1, 2, 3, 4]})
            df["code"] = code
            return df[pdutil.sort_columns(
                df.columns,
                forward=["code", "date", "year", "month", "qtr"],
                drop=["type_id"])]

        with ThreadPool(processes=16) as pool:
            results = pool.map(transform_one, self.codes)
            results = [r for r in results if r is not None]

        result = pd.concat(results)
        return result

    def make_table(self, db_name: str = "finance", table_name: str = "fs"):
        self.transform().to_sql(table_name, maria_home(db_name), index=False)

    def reports(self, code: str):
        query = f"""
        select date, type_id, consolidated, count(*) as count
        from `{code}`
        group by date, consolidated;
        """
        return pd.read_sql(query, self.con)

    def update(self, code: str, df: pd.DataFrame):
        if df.empty:
            return

        date_in = ",".join([f"'{x}'" for x in df["date"].unique()])
        consolidated = df["consolidated"].unique()
        assert len(consolidated) == 1
        consolidated = int(consolidated[0])

        with self.con.connect() as con:
            query = f"""
            delete from `{code}`
            where consolidated = {consolidated} and date in ({date_in}) 
            """
            con.execute(text(query))
            df.to_sql(code, con, if_exists="append", index=False)
            con.commit()

    def update_all(self, date_from: date, date_to: date):
        num = 0
        for code in self.codes:
            num += 1
            _logger.info(f"[{num}] {code}")
            for consolidated in [True, False]:
                df = GetFinancialStatements.call_api(
                    code=code,
                    consolidated=consolidated,
                    date_from=date_from,
                    date_to=date_to
                )

                if df.empty:
                    continue

                df["date"] = pd.to_datetime(df["date"]).dt.date
                df["consolidated"] = consolidated
                assert df["symbol"].nunique() == 1
                df = df.drop(columns=["symbol", "entity_name"])  # 불필요한 칼럼 제거
                df = df.dropna()
                self.update(code, df)
