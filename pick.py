from core.fs import FsLoader
from core.repository.krx import get_ohlcv_latest
from utils import pdutil

table = get_ohlcv_latest().set_index("code")
fin_loader = FsLoader()
table = table.join(fin_loader.load(2024, 1))
table = table.rename(columns={
    "cap": "P",
})

table["EQ/P"] = table["EQ"] / table["P"]
table["R/P"] = table["R/Y"] / table["P"]
table["GP/P"] = table["GP/Y"] / table["P"]
table["O/P"] = table["O/Y"] / table["P"]
table["E/P"] = table["E/Y"] / table["P"]

recipes = {
    "벨류": {
        "GP/P": 0.120550,
        "EQ/P": 0.105678,
    },
    "성장": {
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
    "_전략": {
        "가격": 1,
        "벨류": 1,
        "성장": 1
    },
    "퀄리티": {
        "O/EQ": 5,
        "EBT/EQ": 4,
        "E/EQ": 3,
    },
    "성장2": {
        "O_QoQ": 2,
        "EBT_QoQ": 1,
    },
    "전략": {
        "퀄리티": 2,
        "성장2": 1,
        "O/P": 1
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

table["tags"] = ""


def append_tag(selector, f: str):
    table.loc[selector, "tags"] = table.loc[selector, "tags"].apply(lambda x: x + f", {f}" if x else f)


append_tag(table["open"] == 0, "거래정지")
append_tag(table["E/Y"] < 0, "순이익 적자")
append_tag(table["O/Y"] < 0, "영업이익 적자")

for quanlity_factor in ["GP/A", "GP/EQ", "R/A", "GP/R", "O/A", "E/R", "EBT/A", "O/EQ", "O/R", "R/EQ", "EBT/R", "E/A",
                        "EBT/EQ", "E/EQ"]:
    append_tag(table[f"{quanlity_factor}_pct"] < 0.10, f"저 {quanlity_factor}")

factor = "전략"
table = table.sort_values(f"{factor}_pct", ascending=False)
# table[["전략_pct", "name", "close", "벨류_pct", "성장_pct", "P", "tags"]].to_csv("pick.csv")
table[
    pdutil.sort_columns(
        table.columns,
        [f"{factor}_pct", "name", "close"] + [f"{k}_pct" for k in recipes[factor].keys()] + ["EBT/EQ_pct", "O_QoQ_pct", "O/P_pct"] + ["tags"]
    )
].to_csv("pick.csv")
print("Done.")
