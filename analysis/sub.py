import pandas as pd

from util import normalize


def calc_factor(df, pk, pw, bk, bw):
    result = pd.DataFrame()
    for before_date, _ in sorted(set([tuple(x) for x in df[["before_date", "after_date"]].values])):
        sub_df = df[df["before_date"] == before_date]
        colname_factor = f"{pk}^{pw}/{bk}^{bw}/P"
        colname_n_factor = f"Normalize({colname_factor})"
        colname_r_factor = f"Rank({colname_factor})"
        factor = pow(sub_df[pk], pw) / pow(sub_df[bk], bw) / sub_df["before_cap"]

        n_factor = normalize(factor)
        r_factor = factor.rank(ascending=False)
        factor = factor

        temp = pd.DataFrame(
            {
                colname_factor: factor,
                colname_n_factor: n_factor,
                colname_r_factor: r_factor
            }
        )

        result = pd.concat([result, temp])

    return result
