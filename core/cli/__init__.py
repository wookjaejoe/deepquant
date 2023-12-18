import pandas as pd


def from_bok():
    """
    한국 은행 데이터
    """
    df = pd.read_excel("~/Downloads/OECD 경기선행지수.xlsx")
    df = df.iloc[6:, [0, 2]].dropna()
    df.columns = ["cli_pos", "cli"]
    df.index = pd.to_datetime(df["cli_pos"]) + pd.offsets.MonthBegin(2) - pd.DateOffset(days=1)
    df.index.name = "date"
    df["cli_change"] = df["cli"].pct_change()
    return df


def from_oecd():
    df = pd.read_csv("~/Downloads/DP_LIVE_07122023073629605.csv")
    df = df[["TIME", "Value"]]
    df.columns = ["cli_pos", "cli"]
    df.index = pd.to_datetime(df["cli_pos"]) + pd.offsets.MonthBegin(2) - pd.DateOffset(days=1)
    df.index.name = "date"
    df["cli_change"] = df["cli"].pct_change()
    return df


