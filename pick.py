import numpy as np

from core.repository import load_financial
from core.repository.krx import get_ohlcv_latest
from core.strategy import recipe

colname_alias = {
    "cap": "P",
    "자산총계": "A",
    "자본총계": "EQ",
    "매출액": "R",
    "매출총이익": "GP",
    "영업이익": "O",
    "당기순이익": "E",
    "영업활동으로인한현금흐름": "CF"
}

table = get_ohlcv_latest().set_index("code")
table = table.join(load_financial(2023, 5))
table.update(table[table["확정실적"].notna()]["확정실적"].apply(lambda x: str(x)))
table.rename(columns=colname_alias, inplace=True)

table["GP/P"] = table["GP"] / table["P"]
table["EQ/P"] = table["EQ"] / table["P"]

recipes = {
    "벨류": {
        "GP/P": 1,
        "EQ/P": 1,
    },
    "성장": {
        "R_QoQ": 1,
        "GP_QoQ": 1,
        "O_QoQ": 1,
        "E_QoQ": 1,

        "R/A_QoQ": 1,
        "GP/A_QoQ": 1,
        "O/A_QoQ": 1,
        "E/A_QoQ": 1,
    },
    "recipe": recipe,
    "recipe2": {
        "P": -1,
        "벨류": 1,
        "성장": 1
    }
}

# 개별 팩터들의 pct 계산
factors = ["GP/P", "EQ/P", "P", "BIS"]
factors += [f"{k}_QoQ" for k in ["R", "GP", "O", "E"]]
factors += [f"{k}/A_QoQ" for k in ["R", "GP", "O", "E"]]
for x1 in ["R", "GP", "O", "E"]:
    for x2 in ["A", "EQ"]:
        factor = f"{x1}/{x2}"
        factors.append(factor)
        table[factor] = table[x1] / table[x2]

for factor in factors:
    table[f"{factor}_pct"] = table[factor].rank(method="min", pct=True)


def weighted(pct: float, w: float):
    return pct * w if w > 0 else (1 - pct) * abs(w)


for name, recipe in recipes.items():
    # 1. 레시피를 구성하는 개별 팩터 분위(percentile) * 가중치의 총합을 구함
    sv = sum([weighted(table[f"{k}_pct"], w) for k, w in recipe.items()])
    # 2. 위의 시리즈에 가중치의 총합을 나눈다 => 0~1 사이 값으로 일반화됨
    sv = sv / sum([abs(w) for w in recipe.values()])
    # 3. 표준정규화
    table[name] = sv
    table[f"{name}_rank"] = np.ceil(table[name].rank(ascending=False, method="min"))
    table[f"{name}_pct"] = table[name].rank(method="min", pct=True)

table = table.sort_values("recipe", ascending=False)

table["tags"] = ""


def append_tag(selector, f: str):
    table.loc[selector, "tags"] = table.loc[selector, "tags"].apply(lambda x: x + f", {f}" if x else f)


append_tag(table["R/A_pct"] < 0.1, "저 R/A")
append_tag(table["GP/A_pct"] < 0.1, "저 GP/A")
append_tag(table["O/A_pct"] < 0.1, "저 O/A")
append_tag(table["E/A_pct"] < 0.1, "저 E/A")

append_tag(table["GP/EQ_pct"] < 0.1, "저 R/EQ")
append_tag(table["GP/EQ_pct"] < 0.1, "저 GP/EQ")
append_tag(table["O/EQ_pct"] < 0.1, "저 O/EQ")
append_tag(table["E/EQ_pct"] < 0.1, "저 E/EQ")

append_tag(table["name"].str.contains("홀딩스"), "홀딩스")

table = table[["recipe_rank", "name", "close", "벨류_pct", "성장_pct", "tags"]]
table.to_csv("pick.csv")
print("Done.")
