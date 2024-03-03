import pandas as pd

from core.fs import FsLoader
from core.repository.krx import get_ohlcv_latest
from utils import pdutil

fin_loader = FsLoader.instance()
table = get_ohlcv_latest().set_index("code")
table = table.join(fin_loader.load(2023, 3))
table = table.rename(columns={
    "cap": "P",
})

table["EQ/P"] = table["EQ"] / table["P"]
table["R/P"] = table["R/Y"] / table["P"]
table["GP/P"] = table["GP/Y"] / table["P"]
table["O/P"] = table["O/Y"] / table["P"]
table["E/P"] = table["E/Y"] / table["P"]

#####################
# 매출액 또는 손익구조 변경
from core.dartx.매출액또는손익구조변경 import calc

changed = pd.read_csv("매출액또는손익구조변경2.csv", dtype={"code": str})
changed = changed.set_index("code")
changed = (
    changed[[
        "O_QoQ_pct",
        "E_QoQ_pct",
        "EBT_QoQ_pct",

        "O/EQ_QoQ_pct",
        "E/EQ_QoQ_pct",
        "EBT/EQ_QoQ_pct",
    ]]
    .sum(axis=1)
    .to_frame("성장2")
)
table = table.join(changed)
table["성장2_pct"] = table["성장2"].rank(pct=True)
#####################

recipes = {
    "벨류": {
        "GP/P": 0.120550,
        "EQ/P": 0.105678,
    },
    "성장1": {
        "O_QoQ": 0.027,
        "E_QoQ": 0.024,
        "EBT_QoQ": 0.017,
        "O/A_QoQ": 0.013,
        "E/EQ_QoQ": 0.011,
        "O/EQ_QoQ": 0.01,
        "E/A_QoQ": 0.009,
        "EBT/A_QoQ": 0.008,
        "EBT/EQ_QoQ": 0.006,
        "GP/EQ_QoQ": 0.004,
        "GP/A_QoQ": 0.004,
        "GP_QoQ": 0.003,
        "R_QoQ": 0.001,
        "R/EQ_QoQ": 0.001,
        "R/A_QoQ": 0.001,
    },
    "가격": {
        "P": -1
    },
    "성장": {
        "성장1": 1,
        "성장2": 1
    },
    "전략": {
        "벨류": 1,
        "성장": 1,
        "가격": 1
    }
}

# 개별 팩터들의 pct 계산
factors = ["GP/P", "O/P", "E/P", "EQ/P", "P"]
factors += [f"{k}_QoQ" for k in ["R", "GP", "O", "EBT", "E"]]
factors += [f"{k}/A_QoQ" for k in ["R", "GP", "O", "EBT", "E"]]
factors += [f"{k}/EQ_QoQ" for k in ["R", "GP", "O", "EBT", "E"]]
for x1 in ["R", "GP", "O", "EBT", "E"]:
    for x2 in ["A", "EQ"]:
        factor = f"{x1}/{x2}"
        factors.append(factor)
        table[factor] = table[f"{x1}/Y"] / table[x2]

for x1 in ["GP", "O", "EBT", "E"]:
    factor = f"{x1}/R"
    factors.append(factor)
    table[factor] = table[f"{x1}/Y"] / table["R/Y"]

table = table[table["확정실적"].notna() & table["확정실적"].notnull()]

for factor in factors:
    table[f"{factor}_pct"] = table[factor].rank(pct=True)

for name, recipe in recipes.items():
    table[name] = sum([table[f"{k}_pct"] * w for k, w in recipe.items()])
    table[f"{name}_pct"] = table[name].rank(pct=True)

# Attach tags ##############################
table["tags"] = ""


def append_tag(selector, f: str):
    table.loc[selector, "tags"] = table.loc[selector, "tags"].apply(lambda x: x + f", {f}" if x else f)


append_tag(table["open"] == 0, "거래정지")

quality_factors = [
    "GP/A", "GP/EQ", "R/A", "GP/R", "O/A", "E/R", "EBT/A", "O/EQ", "O/R", "R/EQ", "EBT/R", "E/A",
    "EBT/EQ", "E/EQ"
]
for qf in quality_factors:
    append_tag(table[f"{qf}_pct"] < 0.10, f"저 {qf}")

table = table.sort_values("전략_pct", ascending=False)
table["시총"] = (table["P"] / 1_0000_0000).apply(lambda x: f"{round(x)}억")
table["전략_pct"] = table["전략_pct"].rank(ascending=False)


def flat(k):
    result = []
    if k in recipes.keys():
        result += [k]
        for sub_key in recipes[k].keys():
            result += flat(sub_key)

    return result


fwd = ["시총", "전략_pct", "name", "close"] + [f"{f}_pct" for f in ["전략", "성장1", "성장2", "벨류", "가격"]] + ["tags"]
table[fwd].to_csv("pick.csv")
print("Done.")
