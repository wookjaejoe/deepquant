import os
import pickle

import pandas as pd

accounts = {
    "ifrs-full_Revenue": "매출액",
    "ifrs-full_GrossProfit": "매출총이익",
    "dart_OperatingIncomeLoss": "영업이익",
    "ifrs-full_ProfitLoss": "당기순이익",
    "ifrs-full_Assets": "자산총계",
    "ifrs-full_Liabilities": "부채총계",
    "ifrs-full_Equity": "자본총계",
}


def find_value_colname(df: pd.DataFrame):
    for col in df.columns:
        if "20230331" in col[0] and "연결" in col[1][0]:
            return col

    for col in df.columns:
        if "20230331" in col[0]:
            return col

    raise RuntimeError("Failed to find a value column.")


def find_concept_id_colname(df: pd.DataFrame):
    for col in df.columns:
        if col[1] == "concept_id":
            return col


def adjust(df: pd.DataFrame):
    cn_title = find_concept_id_colname(df)
    cn_value = find_value_colname(df)
    df = df[[cn_title, cn_value]]
    df.columns = ["title", "value"]
    df = df.assign(title=df["title"].map(accounts)).dropna()
    return df


def extract(file: str):
    with open(file, "rb") as r:
        result = pickle.load(r)

    df = pd.concat([adjust(result["fin"]), adjust(result["inc"])]).set_index("title").T
    df["code"] = os.path.basename(file).split(".")[0]
    df = df.set_index("code")
    return df


def main():
    result = pd.DataFrame()
    files = list(os.listdir("2023-1Q"))
    for file in files:
        file = os.path.join("2023-1Q", file)

        try:
            df = extract(file)
            result = pd.concat([result, df])
        except:
            print(f"[WARN] extraction failure: {file}")

    result.to_csv("2023-1Q.csv")
