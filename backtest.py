import pandas as pd

from repository.deepsearch import load_by_year
from repository.maria import chart
import pandas


# todo 주가, 시가총액 수집/적재/조회 - 월봉이어도 상관없을듯 - 이건 stocktock에서 빼서 별도 서버에서 다시 관리하자.
# todo: 매년 3/31 종가기준 직전년도 지표 기반 종목 선정
# 1. 매출총이익/총자산(자산) GP/A
# 2. 매출총이익/순자산(자본) GP/E
# 3. 매출액/총자산(자산)
# 4. 매출액/순자산(자본)
# 5. 매출총이익/시가총액

# 매출총이익/총자산(자산)
def run(year: int, q_size: int):
    print(f'Back-testing ref={year}, act={year + 1}, q_size={q_size}')
    df = load_by_year('매출액', year)
    df = pd.merge(df, load_by_year('매출원가', year), left_index=True, right_index=True)
    df = pd.merge(df, load_by_year('자산', year), left_index=True, right_index=True)
    df['매출총이익'] = df['매출액'] - df['매출원가']
    df['GP/A'] = df['매출총이익'] / df['자산']
    df.index = [symbol.split(':')[1] for symbol in df.index]

    # 변동율 칼럼 추가
    before = chart.get_month_chart(year + 1, 3)
    # 거래 없는 종목 제거
    before = before[before.apply(lambda x: x['vol'] != 0, axis=1)]
    after = chart.get_month_chart(year + 2, 3)
    change = ((after['close'] - before['close']) / before['close']).to_frame(name='change')
    df = pd.merge(df, change, left_index=True, right_index=True)

    # GP/A 기준 정렬
    df = df.sort_values(by='GP/A')

    # 분위 clustering
    q_size = q_size
    quarts = pandas.qcut(
        df['GP/A'],
        q=[(1 / q_size) * i for i in range(q_size)] + [1],
        labels=[i + 1 for i in range(q_size)]
    ).to_frame('Q')
    df = pandas.merge(df, quarts, left_index=True, right_index=True)

    # nan value 제거
    df = df.dropna()

    # 분위별 수익율 평균
    mean = pandas.DataFrame(
        {
            f"{year + 1}/{year}":
                [df.loc[df['Q'] == i + 1]['change'].mean() for i in range(q_size)]
        },
        index=[f"Q{i + 1}" for i in range(q_size)]
    )

    # 분위별 수익율 중간값
    median = pandas.DataFrame(
        {
            f"{year + 1}/{year}":
                [df.loc[df['Q'] == i + 1]['change'].median() for i in range(q_size)]
        },
        index=[f"Q{i + 1}" for i in range(q_size)]
    )

    return mean
    # todo: pressure(거래량), 모멘텀(상승) 기반???


def main():
    # n년 지표데이터, 1년 수익율(n+2년 3월종가-n+1년 3월종가)
    # 1996 - 2021
    fromyear = 1996
    toyear = 2020
    q_size = 20
    result = pandas.DataFrame(index=[f"Q{i + 1}" for i in range(q_size)])
    for year in range(fromyear, toyear + 1):
        partial_result = run(year, q_size)
        result = pandas.merge(result, partial_result, left_index=True, right_index=True)

    result = result.round(4)
    result.to_csv('result.csv')

    cumulative_product = (result + 1).cumprod(axis=1).round(4)
    cumulative_product.to_csv('result_cumulative.csv')

    mean = result.mean(axis=1).round(4)
    mean.to_csv('result_mean.csv')

    # todo: !! 내년 3월까지 기다릴 수 없으니, 직전 4분기 데이터로 백테스트 할 수 있어야 함..
    # todo: 시장 변동율 추가 - 결과 index에 지수 추가하면 될듯
    # todo: 종목 제외 적용 - (거래량 0인 종목 제외), 금융주 제외, 지주사 제외, 관리종목 제외, 적자기업 제외, 중국기업 제외
    # todo: mean, meadian 차이가 큰 이유는?
    # todo: 레포트 더 다양하게 - 년도별 각 분위에 포함된 종목은? 종목의 변동율은? run 함수 안에서 중간결과를 레포팅
    # todo: 월차트 저장


if __name__ == '__main__':
    main()
