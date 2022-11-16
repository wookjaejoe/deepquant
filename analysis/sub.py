import pandas as pd

from util import normalize


def calc_factor(df, pk, pw, bk, bw):
    ff = pd.DataFrame()
    for before_date, _ in sorted(set([tuple(x) for x in df[["before_date", "after_date"]].values])):
        sub_df = df[df["before_date"] == before_date]
        colname_factor = f"{pk}^{pw}/{bk}^{bw}/P"
        colname_n_factor = f"Normalize({colname_factor})"
        colname_r_factor = f"Rank({colname_factor})"
        factor = pow(sub_df[pk], pw) / pow(sub_df[bk], bw) / sub_df["before_cap"]
        temp = pd.DataFrame(
            {
                colname_factor: factor,
                colname_n_factor: normalize(factor),
                colname_r_factor: factor.rank(ascending=False),
                "revenue": sub_df["revenue"]
            }
        )

        ff = pd.concat([ff, temp])

    x_label = ff.columns[2]
    y_label = "revenue"
    ff = ff.dropna()
    ranks = sorted(ff[x_label].unique())
    return (pk, pw, bk, bw), [ff[ff[x_label] == rank][y_label].mean() for rank in ranks]


def mean_top(df: pd.DataFrame, factor_verfication: pd.DataFrame) -> str:
    def _unpack_factor_name(s: str):
        s = s.split("(")[1]
        s = s.split(")")[0]
        profit = s.split("/")[0]
        base = s.split("/")[1]
        print(",".join([profit.split("^")[0], profit.split("^")[1], base.split("^")[0], base.split("^")[1]]))

    def _mean(s: pd.Series, n: int):
        return round(s[:n].mean(), 4)

    x_label = factor_verfication.columns[2]
    y_label = "revenue"
    x = sorted(set(factor_verfication[x_label]))
    y = [df[factor_verfication[x_label] == rank][y_label].mean() for rank in x]
    y_series = pd.Series(y)
    return ",".join([str(x) for x in [_unpack_factor_name(x_label), _mean(y_series, 10)]])


def show_bar():
    # plt.figure(figsize=(20, 8))
    # plt.subplot(1, 2, 1)
    # plt.grid(True)
    # plt.xlabel(x_label)
    # plt.ylabel(y_label)
    # plt.bar(x, y)
    # plt.subplot(1, 2, 2)
    # plt.xlabel(x_label)
    # plt.ylabel(y_label)
    # plt.bar(x, y_series.rolling(10).mean())
    # plt.show()
    pass
