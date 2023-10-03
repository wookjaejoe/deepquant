import numpy as np

from core.fs import FsLoader
from core.repository.krx import get_ohlcv_latest
from utils import pdutil

fin_loader = FsLoader()
table = get_ohlcv_latest().set_index("code")
table = table.join(fin_loader.load(2023, 2))
table = table.rename(columns={
    "cap": "P",
})

table["GP/P"] = table["GP/Y"] / table["P"]
table["EQ/P"] = table["EQ"] / table["P"]

recipes = {
    "벨류": {
        "GP/P": 1,
        "EQ/P": 1,
    },
    "성장": {
        "GP/A_QoQ": 0.142706,
        "O/A_QoQ": 0.130182,
        "GP/EQ_QoQ": 0.129825,
        "O/EQ_QoQ": 0.118514,
        "E/EQ_QoQ": 0.111300,
        "O_QoQ": 0.098340,
        "E_QoQ": 0.097314,
        "GP_QoQ": 0.083276,
        "R/A_QoQ": 0.082624,
        "E/A_QoQ": 0.079270,
        "R/EQ_QoQ": 0.028347,
        "R_QoQ": 0.022196,
    },
    "recipe": {
        "P": -1,
        "벨류": 1,
        "성장": 1
    }
}

# 개별 팩터들의 pct 계산
factors = ["GP/P", "EQ/P", "P"]
factors += [f"{k}_QoQ" for k in ["R", "GP", "O", "E"]]
factors += [f"{k}/A_QoQ" for k in ["R", "GP", "O", "E"]]
factors += [f"{k}/EQ_QoQ" for k in ["R", "GP", "O", "E"]]
for x1 in ["R", "GP", "O", "E"]:
    for x2 in ["A", "EQ"]:
        factor = f"{x1}/{x2}"
        factors.append(factor)
        table[factor] = table[f"{x1}/Y"] / table[x2]

for factor in factors:
    table[f"{factor}_pct"] = table[factor].rank(method="min", pct=True)


def weighted(pct: float, w: float):
    return pct * w if w > 0 else (1 - pct) * abs(w)


for name, recipe in recipes.items():
    # 1. 레시피를 구성하는 개별 팩터 분위(percentile) * 가중치의 총합을 구함
    sv = sum([weighted(table[f"{k}_pct"], w) for k, w in recipe.items()])
    # 2. 위의 시리즈에 가중치의 총합을 나눈다 => 0~1 사이 값으로 일반화됨
    sv = sv / sum([abs(w) for w in recipe.values()])

    table[name] = sv
    table[f"{name}_rank"] = np.ceil(table[name].rank(ascending=False, method="min"))
    table[f"{name}_pct"] = table[name].rank(method="min", pct=True)

table["tags"] = ""


def append_tag(selector, f: str):
    table.loc[selector, "tags"] = table.loc[selector, "tags"].apply(lambda x: x + f", {f}" if x else f)


append_tag(table["open"] == 0, "거래정지")
append_tag(table["R/A_pct"] < 0.10, "저 R/A")
append_tag(table["GP/A_pct"] < 0.10, "저 GP/A")
append_tag(table["O/A_pct"] < 0.10, "저 O/A")
append_tag(table["E/A_pct"] < 0.10, "저 E/A")
append_tag(table["GP/EQ_pct"] < 0.10, "저 R/EQ")
append_tag(table["GP/EQ_pct"] < 0.10, "저 GP/EQ")
append_tag(table["O/EQ_pct"] < 0.10, "저 O/EQ")
append_tag(table["E/EQ_pct"] < 0.10, "저 E/EQ")
append_tag(table["name"].str.contains("홀딩스"), "홀딩스")

table = table.sort_values("recipe_rank")
table[
    pdutil.sort_columns(
        table.columns,
        ["recipe_rank", "name", "open", "close", "P_pct", "벨류_pct", "성장_pct", "tags"]
    )
].to_csv("pick.csv")
print("Done.")
