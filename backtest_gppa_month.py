import os
from datetime import datetime
from typing import *

import pandas as pd

import repository.deepsearch as ds
from repository import get_month_chart
from repository.maria import corp

g = []


def year_month_to_str(year_month: Tuple[int, int]):
    return "/".join([str(v) for v in year_month])


criteria = 'GP/PA'
out_file = f'out-{datetime.now()}.csv'
f = open(out_file, 'a', encoding='utf-8')


def run(year: int, month: int):
    before_year_month = (year, month)
    if month == 12:
        after_year_month = (year + 1, 1)
    else:
        after_year_month = (year, month + 1)

    before = get_month_chart(*before_year_month)
    # 거래정지 종목 제거 - 거래 정지 여부 정확히 알수 없지만 아래 조건은 거래 정지 종목으로 보임
    before = before[before.apply(lambda x: x['cap'] != 0, axis=1)]
    before = before[before.apply(lambda x: x['vol'] != 0, axis=1)]
    # 내년 차트 조회
    after = get_month_chart(*after_year_month)
    if len(after.index) == 0:
        after = before

    # 변동율
    change = ((after['close'] - before['close']) / before['close']).to_frame(name='수익율')

    # nan 처리
    # before: o, after: x -> 거래정지
    only_left = before.index.join(after.index, how='outer').drop(before.index.join(after.index, how='right'))
    for code in only_left:
        change.loc[code] = -0.99  # 거래중단 -> 수익율 -100% 가정

    # before: x, after: o -> 신규상장. 제거
    change = change.dropna()

    # 재무데이터 로드
    df = sum(ds.load('매출액', year, month))
    df = pd.merge(df, sum(ds.load('매출원가', year, month)), left_index=True, right_index=True)
    df = pd.merge(df, sum(ds.load('당기순이익', year, month)), left_index=True, right_index=True)
    df = pd.merge(df, ds.load('자산', year, month)[0], left_index=True, right_index=True)
    # 코드 표준화
    df.index = [symbol.split(':')[1] for symbol in df.index]
    df['매출총이익'] = df['매출액'] - df['매출원가']
    df['GP/PA'] = df['매출총이익'] / (before['cap'] * df['자산'])
    # df['GP/A'] = df['매출총이익'] / df['자산']
    # df['GP/P'] = df['매출총이익'] / before['cap']
    # df['GP/PAE'] = df['매출총이익'] / (before['cap'] * df['자산'] * df['당기순이익'])
    # df['GPGP/PA'] = df['매출총이익'].pow(2) / (before['cap'] * df['자산'])
    # df['GPGP/PAE'] = df['매출총이익'].pow(2) / (before['cap'] * df['자산'] * df['당기순이익'])
    df = df.join(((df[criteria] - df[criteria].mean()) / df[criteria].std()).to_frame(f'norm({criteria})'))
    df = df.join(before['close'].to_frame(f'종가매수({year_month_to_str(before_year_month)})'))
    df = df.join(after['close'].to_frame(f'종가매도({year_month_to_str(after_year_month)})'))
    df = df.join(before['cap'].to_frame(f'cap({year_month_to_str(before_year_month)})'))

    # 정렬
    df = df.sort_values(by=criteria, ascending=False)
    df = pd.merge(df, change, left_index=True, right_index=True)

    # nan value 제거
    df = df.dropna()

    # 종목명에 홀딩스, 지주 포함 종목 제거
    filtered_index = [code for code in df.index if
                      corp.exists(code) and '지주' not in corp.get_name(code) and '홀딩스' not in corp.get_name(code)]
    df = df.filter(items=filtered_index, axis=0)

    # Add ranking
    df = df.join(df[criteria].rank(ascending=False).to_frame(f'{criteria} Rank'))
    df['name'] = [corp.get_name(code) for code in df.index]
    return df


def main():
    # n년 지표데이터, 1년 수익율(n+2년 3월종가-n+1년 3월종가)
    # 1996 - 2021
    fromyear = 2021
    toyear = 2022
    final_amount = 1
    for year in range(fromyear, toyear + 1):
        if year == 2006:
            months = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        else:
            months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

        for month in months:
            print(f"{year}년 {month}월")
            df = run(year, month)
            top = df[:10]
            major_colums = ["GP/PA Rank", "name", "수익율"]
            top = top[major_colums + [col for col in top.columns if col not in major_colums]]
            final_amount = (final_amount * (top['수익율'].mean() + 1)).round(3)
            print(top)
            print(final_amount)
            f.write(
                os.linesep.join(
                    [
                        f"{year}년 {month}월",
                        top.to_csv(),
                        f"누적금액: {final_amount.round(3)}, 최초 투입금을 1로 가정",
                        "",
                        "",
                        ""
                    ]
                )
            )


if __name__ == '__main__':
    main()

f.close()
