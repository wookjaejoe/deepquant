from core.fs import FsLoader
from core.repository.krx import get_ohlcv_latest
from utils import pdutil

fin_loader = FsLoader()
table = get_ohlcv_latest().set_index("code")
table = table.join(fin_loader.load(2023, 3))
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
    "퀄리티": {
        "GP/EQ": 0.081688,
        "GP/A": 0.068516,
        "O/A": 0.059149,
        "O/R": 0.056152,
        "O/EQ": 0.054418,
        "R/EQ": 0.053285,
        "EBT/A": 0.042171,
        "EBT/R": 0.035214,
        "GP/R": 0.034800,
        "EBT/EQ": 0.034718
    },
    "성장": {
        "GP/A_QoQ": 0.142706,
        "O/A_QoQ": 0.130182,

        # "GP/EQ_QoQ": 0.129825,
        # "O/EQ_QoQ": 0.118514,
        # "E/EQ_QoQ": 0.111300,

        "O_QoQ": 0.098340,
        "E_QoQ": 0.097314,
        "GP_QoQ": 0.083276,
        "R/A_QoQ": 0.082624,
        "E/A_QoQ": 0.079270,
    },
    "recipe": {
        "P": -1,
        "벨류": 1,
        "성장": 1,
        "퀄리티": 0.5
    }
}

# 개별 팩터들의 pct 계산
factors = ["GP/P", "EQ/P", "P"]
factors += [f"{k}_QoQ" for k in ["R", "GP", "O", "E"]]
factors += [f"{k}/A_QoQ" for k in ["R", "GP", "O", "E"]]
factors += [f"{k}/EQ_QoQ" for k in ["R", "GP", "O", "E"]]
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
append_tag(table["R/A_pct"] < 0.10, "저 R/A")
append_tag(table["GP/A_pct"] < 0.10, "저 GP/A")
append_tag(table["O/A_pct"] < 0.10, "저 O/A")
append_tag(table["E/A_pct"] < 0.10, "저 E/A")
append_tag(table["GP/EQ_pct"] < 0.10, "저 R/EQ")
append_tag(table["GP/EQ_pct"] < 0.10, "저 GP/EQ")
append_tag(table["O/EQ_pct"] < 0.10, "저 O/EQ")
append_tag(table["E/EQ_pct"] < 0.10, "저 E/EQ")
append_tag(table["name"].str.contains("홀딩스"), "홀딩스")

table = table.sort_values("recipe_pct", ascending=False)
table[
    pdutil.sort_columns(
        table.columns,
        ["recipe_pct", "name", "open", "close", "P_pct", "벨류_pct", "성장_pct", "퀄리티_pct", "tags"]
    )
].to_csv("pick.csv")
print("Done.")
