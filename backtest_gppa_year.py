import pandas as pd

from repository.deepsearch import load_by_year
from repository.maria import chart, corp
import pandas
from typing import *

# todo 주가, 시가총액 수집/적재/조회 - 월봉이어도 상관없을듯 - 이건 stocktock에서 빼서 별도 서버에서 다시 관리하자.
# todo: 매년 3/31 종가기준 직전년도 지표 기반 종목 선정
# 1. 매출총이익/총자산(자산) GP/A
# 2. 매출총이익/순자산(자본) GP/E
# 3. 매출액/총자산(자산)
# 4. 매출액/순자산(자본)
# 5. 매출총이익/시가총액
# 매출총이익/총자산(자산)

g = []

def year_month_to_str(year_month: Tuple[int, int]):
    return "/".join([str(v) for v in year_month])


def run(year: int, q_size: int):
    before_year_month = (year + 1, 3)
    after_year_month = (year + 2, 3)
    criteria = 'GP/PA'
    print(f'[{criteria}] 재무데이터 참조년도={year}/12, 매수년월={year + 1}/03, 매도년월={year + 2}/03')
    before = chart.get_month_chart(*before_year_month)
    # 거래정지 종목 제거 - 거래 정지 여부 정확히 알수 없지만 아래 조건은 거래 정지 종목으로 보임
    before = before[before['cap'] != 0]
    before = before[before['vol'] != 0]
    # 내년 차트 조회
    after = chart.get_month_chart(*after_year_month)
    # 내년/작년 변동율
    change = ((after['close'] - before['close']) / before['close']).to_frame(name='change')

    # nan 처리
    # before: o, after: x -> 거래정지
    only_left = before.index.join(after.index, how='outer').drop(before.index.join(after.index, how='right'))
    for code in only_left:
        change.loc[code] = -0.99  # 거래중단 -> 수익율 -100% 가정 fixme

    # before: x, after: o -> 신규상장. 제거
    change = change.dropna()

    # 재무데이터 로드
    df = load_by_year('매출액', year)
    df = pd.merge(df, load_by_year('매출원가', year), left_index=True, right_index=True)
    df = pd.merge(df, load_by_year('자산', year), left_index=True, right_index=True)
    df = pd.merge(df, load_by_year('당기순이익', year), left_index=True, right_index=True)
    # df = df[df['당기순이익'] > 0]  fixme 이건 또 왜이럼?

    df.index = [symbol.split(':')[1] for symbol in df.index]

    df['매출총이익'] = df['매출액'] - df['매출원가']
    df['GP/A'] = df['매출총이익'] / df['자산']
    df['GP/P'] = df['매출총이익'] / before['cap']
    df['GP/P+A'] = df['매출총이익'] / (before['cap'] + df['자산'])  # todo: 이거 머임??? 수익율이???
    df['GP/PA'] = df['매출총이익'] / (before['cap'] * df['자산'])
    df['GPGP/PA'] = (df['매출총이익'] * df['매출총이익']) / (before['cap'] * df['자산'])
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

    global g

    if len(g) == 0:
        g = [1] * 8

    # head
    g[0] *= (1 + change.filter(items=df[criteria].index[:10].tolist(), axis=0)['change'].mean())
    g[1] *= (1 + change.filter(items=df[criteria].index[:30].tolist(), axis=0)['change'].mean())
    g[2] *= (1 + change.filter(items=df[criteria].index[:50].tolist(), axis=0)['change'].mean())

    # buffer size 10
    g[3] *= (1 + change.filter(items=df[criteria].index[10:30].tolist(), axis=0)['change'].mean())
    g[4] *= (1 + change.filter(items=df[criteria].index[30:50].tolist(), axis=0)['change'].mean())
    g[5] *= (1 + change.filter(items=df[criteria].index[50:100].tolist(), axis=0)['change'].mean())

    # total, 하위
    g[6] *= (1 + change['change'].mean())
    g[7] *= (1 + change.filter(items=df[criteria].index[-100:].tolist(), axis=0)['change'].mean())

    print({i: v.round(2) for i, v in enumerate(g)})

    # interest = change.filter(items=df[criteria].head(10).index.tolist(), axis=0)

    df['name'] = [corp.get_name(code) for code in df.index]
    print(',' + ','.join(df.columns))
    for code in df[:10].sort_values(by='change', ascending=False).index.tolist():
        print(','.join(["\"" + code + "\""] + [str(v) for v in df.loc[code].values]))
    print()
    print()

    return df, mean
    # todo: pressure(거래량), 모멘텀(상승) 기반???


def main():
    # n년 지표데이터, 1년 수익율(n+2년 3월종가-n+1년 3월종가)
    # 1996 - 2021
    fromyear = 1996
    toyear = 2020
    q_size = 20

    # fixme 아직도 괴리가 큰데, 재무제표 데이터 존재하는 애들은 수익율 좋고, 재무제표 데이터 없는 애들은 수익율 나쁜거 아니냐?
    # fixme na 처리 안해서? 걍 디버깅해서 run 함수 안에서 벌어지는 일이랑 위 루프문에서 동작이랑 비교하자.
    ###############################################################################

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

    # fixme: CAGR을 구해야함.
    # fixme: 이거 무슨 수익율이 이렇게 높게 나오는데 왜 이런거냐?
    # => 답을 찾은거 같다. 지수는 구성종목 시가총액 총합이기 때문에 대형주 변동율 작고, 소형주 변동율 높으면 변동율 변동 대비 지수의 변화 크지 않음
    # todo: (생존편향) 일단 거래 정지 종목 수익율 -100% 밖았음
    # todo: !! 내년 3월까지 기다릴 수 없으니, 직전 4분기 데이터로 백테스트 할 수 있어야 함..
    # todo: 시장 변동율 추가 - 결과 index에 지수 추가하면 될듯
    # todo: 종목 제외 적용 - (거래량 0인 종목 제외), 금융주 제외, 지주사 제외, 관리종목 제외, 적자기업 제외, 중국기업 제외
    # todo: mean, meadian 차이가 큰 이유는?
    # todo: 레포트 더 다양하게 - 년도별 각 분위에 포함된 종목은? 종목의 변동율은? run 함수 안에서 중간결과를 레포팅
    # todo: 월차트 저장


if __name__ == '__main__':
    main()
