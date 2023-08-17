import os
import json
import pandas as pd

resource_folder = "2023-2Q"


def find_column(df: pd.DataFrame, *contains: str):
    for col in df.columns:
        if all([item in col for item in contains]):
            return col

    return None


def load_bs(df: pd.DataFrame):
    col_key = find_column(df, "concept_id")
    col_value = find_column(df, "재무제표")

    if len([col for col in [col_key, col_value] if col]) != 2:
        return None

    return df[[col_key, col_value]].set_index(col_key)[col_value]


def load_is(df: pd.DataFrame):
    col_key = find_column(df, "concept_id")
    col_value = find_column(df, "재무제표")

    if len([col for col in [col_key, col_value] if col]) != 2:
        return None

    return df[[col_key, col_value]].set_index(col_key)[col_value]


def load_one(folder: str, filename: str):
    filepath = os.path.join(folder, filename)
    with open(filepath) as f:
        data = json.load(f)

    return pd.concat([
        load_bs(pd.DataFrame(data["bs"])) if data["bs"] else None,
        load_is(pd.DataFrame(data["is"])) if data["is"] else None,
        load_bs(pd.DataFrame(data["cf"])) if data["cf"] else None
    ])


def main():
    accounts = {
        "ifrs-full_CurrentAssets": "유동자산",
        "ifrs-full_Assets": "자산총계",
        "ifrs-full_Equity": "자본총계",
        "ifrs-full_Revenue": "매출액",
        "ifrs-full_GrossProfit": "매출총이익",
        "dart_OperatingIncomeLoss": "영업이익",
        "ifrs-full_ProfitLoss": "당기순이익",
        "ifrs-full_CashFlowsFromUsedInOperatingActivities": "영업활동현금흐름"
    }

    df = pd.DataFrame()
    count = 1
    # for filename in ["005930.json"]:
    for filename in os.listdir(resource_folder):
        if "sep" in filename:
            continue

        print(f"[{count}] {filename}")
        code = filename.split(".")[0].split("_")[0]
        try:
            row = load_one("2023-2Q", filename)
            row = row[[i for i in row.index if i in accounts.keys()]]
            df = pd.concat([df, row.to_frame(code).T])
        except Exception as e:
            print(f"[WARN] {e}")
        finally:
            count += 1

    return df
