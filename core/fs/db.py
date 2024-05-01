import logging
from datetime import date
from typing import Callable

import pandas as pd
from sqlalchemy import text

from core.ds import GetFinancialStatements
from core.ds.exceptions import AuthenticationError
from core.repository import maria_home
from utils import pdutil

_logger = logging.getLogger()

_default_accounts = {
    # 재무상태표
    "11:5000": "자산총계",
    "11:8900": "자본총계",
    "11:2000": "유동자산",
    "11:6000": "유동부채",

    # 손익계산서
    "12:1000": "매출",
    "12:3000": "매출총이익",
    "12:5000": "영업이익",
    "12:8000": "법인세비용차감전계속영업이익",
    "12:8200": "당기순이익",
    "12:9000": "당기순이익",

    # 현금흐름표
    "16:1000": "영업활동현금흐름",
}


class FsDb:

    def __init__(self, account: dict = None):
        self.account = account if account is not None else _default_accounts

    @property
    def con(self):
        return maria_home("fs")

    @property
    def codes(self):
        return pd.read_sql("show tables", self.con).iloc[:, 0].to_list()

    def table(self, code: str, con=None) -> pd.DataFrame:
        """
        :param code: 종목코드 6자리 문자열
        :param con: 커넥션 객체, 미 입력시 새로 연결
        """
        if not con:
            con = self.con

        acc = ", ".join([f"'{x}'" for x in self.account.keys()])
        query = f"""
        select * from `{code}`
        where concat(report_id, ':', account_id) in ({acc})
        """
        return pd.read_sql(query, con)

    def distinct_dates(self, code: str):
        """
        :param code: 종목코드 6자리 문자열
        """
        return pd.read_sql(f"select distinct date from `{code}`", self.con)

    def _pivot(self):
        """
        싱글 테이블로 변환
        """
        con = self.con
        results = pd.DataFrame()
        total = len(self.codes)
        num = 0
        for code in self.codes:
            num += 1
            print(f"[{num}/{total}] {code}")

            df = self.table(code, con)
            if df.empty:
                continue

            df["title"] = df[["report_id", "account_id"]].apply(lambda x: self.account[":".join(x)], axis=1)
            df = df.pivot_table(
                index=["date", "type_id", "consolidated"], columns="title", values="value",
                aggfunc=lambda x: x.iloc[-1]
            )
            df = df.reset_index()
            df["year"] = df["date"].apply(lambda x: x.year)
            df["month"] = df["date"].apply(lambda x: x.month)
            df["qtr"] = df["type_id"].map({"F": 1, "B": 2, "T": 3, "K": 4})
            df["code"] = code
            result = df[pdutil.sort_columns(
                df.columns,
                forward=["code", "date", "year", "month", "qtr"],
                drop=["type_id"])]

            if result is not None:
                results = pd.concat([results, result])

        return results

    def make_table(self, db_name: str = "finance", table_name: str = f"fs_{date.today()}"):
        db = maria_home(db_name)
        with db.connect() as con:
            f"""
            create table {table_name} 
            (
                code           varchar(6) not null,
                date           date       null,
                year           int        not null,
                month          tinyint    not null,
                qtr            tinyint    not null,
                consolidated   tinyint    not null,
                매출             bigint     null,
                매출총이익          bigint     null,
                영업이익           bigint     null,
                법인세비용차감전계속영업이익 bigint     null,
                당기순이익          bigint     null,
                자산총계           bigint     null,
                자본총계           bigint     null,
                유동부채           bigint     null,
                유동자산           bigint     null,
                영업활동현금흐름       bigint     null,
                primary key (code, year, qtr, month, consolidated)
            );
            """
            con.commit()

        self._pivot().to_sql(table_name, db, index=False, if_exists="replace")

    def reports(self, code: str):
        query = f"""
        select date, type_id, consolidated, count(*) as count
        from `{code}`
        group by date, consolidated;
        """
        return pd.read_sql(query, self.con)

    def _update(self, code: str, df: pd.DataFrame):
        if df.empty:
            return

        date_in = ",".join([f"'{x}'" for x in df["date"].unique()])
        consolidated = df["consolidated"].unique()
        assert len(consolidated) == 1
        consolidated = int(consolidated[0])

        with self.con.connect() as con:
            # todo: check table exists

            if code in self.codes:
                query = f"""
                delete from `{code}`
                where consolidated = {consolidated} and date in ({date_in}) 
                """
                result = con.execute(text(query))
                _logger.info(f"{result.rowcount} rows deleted.")

            df.to_sql(code, con, if_exists="append", index=False)
            _logger.info(f"{len(df)} rows inserted.")
            con.commit()

    def update_all(
        self,
        codes: list[str],
        date_from: date,
        date_to: date,
        should_update: Callable[[str], bool] = lambda code: True
    ):
        """
        사용 예시)
        db.update_all(
            codes=stocks["stock_code"].tolist(),
            date_from=date(2023, 1, 1),
            date_to=date.today(),
            should_update=lambda code: (2023, 9) not in [(dt.year, dt.month) for dt in db.distinct_dates(code)["date"]]
        )
        """
        num = 0
        for code in codes:
            num += 1
            _logger.info(f"[{num}/{len(codes)}] {code}")
            if not should_update(code):
                _logger.info(f"Skipping...")
                continue

            for consolidated in [True, False]:
                try:
                    df = GetFinancialStatements.call(
                        code=code,
                        consolidated=consolidated,
                        date_from=date_from,
                        date_to=date_to
                    )
                except AuthenticationError:
                    raise
                except Exception as e:
                    _logger.error(f"Failed to call API - {code, consolidated, date_from, date_to}", exc_info=e)
                    continue

                if df.empty:
                    continue

                df["date"] = pd.to_datetime(df["date"]).dt.date
                df["consolidated"] = consolidated
                assert df["symbol"].nunique() == 1
                df = df.drop(columns=["symbol", "entity_name"])  # 불필요한 칼럼 제거
                df = df.dropna()
                self._update(code, df)
