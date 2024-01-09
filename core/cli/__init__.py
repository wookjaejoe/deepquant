import pandas as pd
import requests


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
    url = "https://stats.oecd.org/sdmx-json/data/DP_LIVE/KOR.CLI.AMPLITUD.LTRENDIDX.M/OECD?" + "&".join([
        "json-lang=en",
        "dimensionAtObservation=allDimensions",
        "startPeriod=1900-01",
        "endPeriod=2099-12"
    ])

    res = requests.get(url)
    data = res.json()
    values = [x[0] for x in data["dataSets"][0]["observations"].values()]
    months = [x for x in data["structure"]["dimensions"]["observation"] if x["id"] == "TIME_PERIOD"][0]
    months = [x["id"] for x in months["values"]]
    df = pd.DataFrame({"cli_pos": months, "cli": values})
    df.index = pd.to_datetime(df["cli_pos"]) + pd.offsets.MonthBegin(2) - pd.DateOffset(days=1)
    df.index.name = "date"
    df["cli_change"] = df["cli"].pct_change()
    return df
