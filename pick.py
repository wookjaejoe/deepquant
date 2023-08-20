import numpy as np

from core.repository import FinanceLoader
from core.repository.krx import get_ohlcv_latest
from base.timeutil import YearQuarter
from core.strategy import recipe

fin_loader = FinanceLoader()
table = get_ohlcv_latest().set_index("code")
table = table.join(fin_loader.load(YearQuarter(2023, 2)))
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
    "성장_단순이익": {f"{e}_QoQ": 1 for e in ["R", "GP", "O", "E"]},
    "성장_자산대비이익": {f"{e}/A_QoQ": 1 for e in ["R", "GP", "O", "E"]},
    "성장_자본대비이익": {f"{e}/EQ_QoQ": 1 for e in ["R", "GP", "O", "E"]},
    "성장_매출종합": {f"R{b}_QoQ": 1 for b in ["", "/A", "/EQ"]},
    "성장_매출총이익종합": {f"GP{b}_QoQ": 1 for b in ["", "/A", "/EQ"]},
    "성장_영업이익종합": {f"O{b}_QoQ": 1 for b in ["", "/A", "/EQ"]},
    "성장_순이익종합": {f"E{b}_QoQ": 1 for b in ["", "/A", "/EQ"]},
    "성장": {
        "성장_매출총이익종합": 5,
        "성장_영업이익종합": 4,
        "성장_순이익종합": 3,
        "성장_매출종합": 2,
    },
    "v3": recipe,
    "v4": {
        "P": -1,
        "벨류": 1,
        "성장": 1
    },
    "recipe": {
        "v3": 1,
        "v4": 1
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
table[["v3_rank", "v4_rank", "recipe_rank", "name", "open", "close", "P_pct", "벨류_pct", "성장_pct", "tags"]].to_csv("pick.csv")
print("Done.")
