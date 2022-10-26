import pandas as pd

from repository.deepsearch import load_by_year, load_all_by, load_by_quart
from repository.maria import chart, corp
import pandas
from typing import *

g = []


def year_month_to_str(year_month: Tuple[int, int]):
    return "/".join([str(v) for v in year_month])


def run(year: int, q_size: int):
    action_month = 10
    before_year_month = (year, action_month)
    after_year_month = (year + 1, action_month)
    criteria = 'GP/PA'

    print(
        f'[{criteria}] 재무데이터 참조년도={year}, 매수년월={before_year_month[0]}/{before_year_month[1]}, 매도년월={after_year_month[0]}/{after_year_month[1]}')
    before = chart.get_month_chart(*before_year_month)
    # 거래정지 종목 제거 - 거래 정지 여부 정확히 알수 없지만 아래 조건은 거래 정지 종목으로 보임
    before = before[before.apply(lambda x: x['cap'] != 0, axis=1)]
    before = before[before.apply(lambda x: x['vol'] != 0, axis=1)]
    # 내년 차트 조회
    after = chart.get_month_chart(*after_year_month)
    if len(after.index) == 0:
        after = before

    # 변동율
    change = ((after['close'] - before['close']) / before['close']).to_frame(name='change')

    # nan 처리
    # before: o, after: x -> 거래정지
    only_left = before.index.join(after.index, how='outer').drop(before.index.join(after.index, how='right'))
    for code in only_left:
        change.loc[code] = -0.99  # 거래중단 -> 수익율 -100% 가정 fixme

    # before: x, after: o -> 신규상장. 제거
    change = change.dropna()

    # 재무데이터 로드
    df = sum(load_all_by('매출액', year, 2, limit=2))
    df = pd.merge(df, sum(load_all_by('매출원가', year, 2, limit=2)), left_index=True, right_index=True)
    df = pd.merge(df, load_by_quart('자산', year, 2), left_index=True, right_index=True)
    df.index = [symbol.split(':')[1] for symbol in df.index]

    df['매출총이익'] = df['매출액'] - df['매출원가']
    df['GP/A'] = df['매출총이익'] / df['자산']
    df['GP/P'] = df['매출총이익'] / before['cap']
    df['GP/PA'] = df['매출총이익'] / (before['cap'] * df['자산'])  # todo: 이거 머임??? 수익율이???
    df = df.join(before['close'].to_frame(f'종가({year_month_to_str(before_year_month)})'))
    df = df.join(after['close'].to_frame(f'종가({year_month_to_str(after_year_month)})'))
    df = df.join(before['cap'].to_frame(f'시총({year_month_to_str(before_year_month)})'))

    # 정렬
    df = df.sort_values(by=criteria, ascending=False)
    df = pd.merge(df, change, left_index=True, right_index=True)

    # nan value 제거
    df = df.dropna()

    # fixme: 임시코드 - 홀딩스랑 지주 빼니까 진짜 수익율 올라가네?
    x = [code for code in df.index if '지주' not in corp.get_name(code) and '홀딩스' not in corp.get_name(code)]
    df = df.filter(items=x, axis=0)
    ############################

    # 분위 clustering
    q_size = q_size
    quarts = pandas.qcut(
        df[criteria],
        q=[(1 / q_size) * i for i in range(q_size)] + [1],
        labels=[i + 1 for i in range(q_size)]
    ).to_frame('Q')
    df = pandas.merge(df, quarts, left_index=True, right_index=True)

    # 분위별 수익율 평균
    col_name = f"{year + 1}/{year}"
    mean = pandas.DataFrame(
        {
            col_name: [df.loc[df['Q'] == i + 1]['change'].mean() for i in range(q_size)]
        },
        index=[f"Q{i + 1}" for i in range(q_size)]
    )
    mean = pandas.concat([mean, pandas.DataFrame({col_name: [df['change'].mean()]}, index=["AVG_AVAILABLE"])])
    whole_average = change.mean()
    whole_average.index = ['AVG_ALL']
    mean = pandas.concat([mean, whole_average.to_frame(col_name)])

    # Add ranking
    df = df.join(df[criteria].rank(ascending=False).to_frame('rank'))
    df = df.join(((df[criteria] - df[criteria].mean()) / df[criteria].std()).to_frame(f'NORM({criteria})'))

    global g

    if len(g) == 0:
        g = [1] * 8

    # head
    g[0] *= (1 + change.filter(items=df[criteria].index[:10].tolist(), axis=0)['change'].mean())
    g[1] *= (1 + change.filter(items=df[criteria].index[:20].tolist(), axis=0)['change'].mean())
    g[2] *= (1 + change.filter(items=df[criteria].index[:30].tolist(), axis=0)['change'].mean())
    g[3] *= (1 + change.filter(items=df[criteria].index[:50].tolist(), axis=0)['change'].mean())
    g[4] *= (1 + change.filter(items=df[criteria].index[:100].tolist(), axis=0)['change'].mean())
    g[5] *= (1 + change.filter(items=df[criteria].index[:200].tolist(), axis=0)['change'].mean())

    # total, 하위
    g[6] *= (1 + change['change'].mean())
    g[7] *= (1 + change.filter(items=df[criteria].index[-100:].tolist(), axis=0)['change'].mean())

    g_log = {
        '1-10': g[0],
        '1-20': g[1],
        '1-30': g[2],
        '1-50': g[3],
        '1-100': g[4],
        '1-200': g[5],
        'All': g[6],
        'last 100': g[7],
    }
    print(','.join([f'{k}: {v}' for k, v in g_log.items()]))

    df['name'] = [corp.get_name(code) for code in df.index]
    print(',' + ','.join(df.columns))
    for code in df[:30].sort_values(by='change', ascending=False).index.tolist():
        print(','.join(["\"" + code + "\""] + [str(v) for v in df.loc[code].values]))
    print()
    print()

    return df, mean


def main():
    # n년 지표데이터, 1년 수익율(n+2년 3월종가-n+1년 3월종가)
    # 1996 - 2021
    fromyear = 2005
    toyear = 2022
    q_size = 20

    result = None
    change = None
    for year in range(fromyear, toyear + 1):
        df, mean = run(year, q_size)
        if change is None:
            change = df['change'].to_frame(year + 1)
        else:
            change = pandas.merge(change, df['change'].to_frame(year + 1), left_index=True, right_index=True,
                                  how='outer')

        if result is None:
            result = mean
        else:
            result = pandas.merge(result, mean, left_index=True, right_index=True)

    result = result.round(4)
    result.to_csv('out/result.csv')

    cumulative_product = (result + 1).cumprod(axis=1) * 100
    cumulative_product.round().to_csv('out/result_cumulative.csv')

    mean = result.mean(axis=1) * 100
    mean.round().to_csv('out/result_mean.csv')

    print({i: v.round(2) for i, v in enumerate(g)})


if __name__ == '__main__':
    main()