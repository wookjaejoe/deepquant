import numpy as np
import pandas as pd

from utils.timeutil import YearMonth
from sklearn.linear_model import LinearRegression
from scipy.optimize import minimize

data = pd.read_csv(".analysis/layer3.csv", dtype={"code": str})
data["매도년월"] = data["매도년월"].apply(YearMonth.from_string)


def heatmap(recipe: dict):
    global data
    yms = data["매도년월"].drop_duplicates().sort_values()
    xl, yl = f"factor_pct", "수익률_pct"
    result = pd.DataFrame()
    for ym in yms:
        df = data[data["매도년월"] == ym].copy()
        df["factor"] = sum([df[f"{k}_pct"] * w for k, w in recipe.items()])
        df[xl] = np.ceil(df["factor"].rank(pct=True) * 10)
        df[yl] = np.ceil(df["수익률"].rank(pct=True) * 8)
        result = pd.concat([result, df[["매도년월", xl, "수익률", yl]]])

    result = result[[xl, yl]].dropna()
    x_values = result[xl]
    y_values = result[yl]
    x_mean, y_mean = x_values.mean(), y_values.mean()
    freq = result.groupby([xl, yl]).size().reset_index(name="freq")
    heatmap_data = freq.pivot(index=xl, columns=yl, values="freq")
    # 팩터값 1일때, 수익률이 1일 확률
    heatmap_data = heatmap_data.apply(lambda row: row / sum(row), axis=1)
    print()


def ml_example(recipe: dict):
    """
    ML - 선형 회귀 모델 학습 통한 가중치 추출
    """
    global data
    yms = data["매도년월"].drop_duplicates().sort_values()
    result = pd.DataFrame()
    for ym in yms:
        df = data[data["매도년월"] == ym].copy()
        df["수익률_pct"] = np.ceil(df["수익률"].rank(pct=True) * 10)
        result = pd.concat([result, df])

    xcs = [f"{k}_pct" for k in recipe.keys()]
    yc = "수익률_pct"
    result = result[xcs + [yc]].dropna()
    model = LinearRegression()
    model.fit(result[xcs], result[yc])
    return model.coef_


def evaluate(recipe: dict):
    """
    corr * slope 평가
    """
    global data
    pct_size = 20
    yms = data["매도년월"].drop_duplicates().sort_values()
    xl, yl = f"factor_pct", "수익률"
    result = pd.DataFrame()
    for ym in yms:
        df = data[data["매도년월"] == ym].copy()
        df["factor"] = sum([df[f"{k}_pct"] * w for k, w in recipe.items()])
        df[xl] = np.ceil(df["factor"].rank(pct=True) * pct_size)
        result = pd.concat([result, df[["매도년월", xl, yl]]])

    result = result.dropna()
    months = result["매도년월"].unique()
    duration = months.min().duration(months.max())
    result = result.groupby(xl).apply(lambda x: (x.groupby("매도년월")["수익률"].mean() + 1).prod())
    result = result ** (1 / duration) - 1
    result = result.reset_index(name=yl)

    x_values, y_values = result[xl], result[yl]
    x_mean, y_mean = x_values.mean(), y_values.mean()

    slope = sum((x_values - x_mean) * (y_values - y_mean)) / sum((x_values - x_mean) ** 2)
    corr = result.corr("spearman")[xl][yl]
    print({k: v for k, v in recipe.items()}, corr * slope)
    return corr * slope


def optimize(recipe: dict):
    """
    scipy 통한 최적화
    """

    # todo: 여기 원래 constraints 를 줄 수 있게 되어 있음. 이걸 어떻게 활용해야 할텐데..
    return minimize(
        lambda x: -evaluate(dict(zip(recipe.keys(), x))),
        x0=np.array(list(recipe.values())),
    ).x


def run():
    # evaluate({
    #     "P": -1,
    #     "GP/P": 1,
    #     "EQ/P": 1
    # })

    # evaluate2({
    #     "P": -1114,
    #     "GP/P": 4225,
    #     "EQ/P": 4295
    # })
    # 0.832797118847539,0.004464287925022362 => 0.0037178461216644814
    # 0.9306602641056423,0.005924828064934072 => 0.005514002051692065

    result = optimize({
        "P": -0.5,
        "GP/P": 0.25,
        "EQ/P": 0.25
    })
    print(result)
