import pandas as pd


def display(value):
    if value >= 10000_0000_0000:
        return f"{round(value / 10000_0000_0000, 2)}조"
    elif value >= 10000_0000:
        return f"{round(value / 10000_0000, 2)}억"
    elif value >= 10000:
        return f"{round(value / 10000, 2)}만"
    else:
        return str(round(value, 4))


def stat(code: str):
    df = pd.read_csv("pick.csv", dtype={"code": str})
    stock = df[df["code"] == code].iloc[0]
    stock_name, market, state = stock[["name", "market_name", "_SECT_TP_NM"]]

    print(f"{stock_name}({code}) - {market} / {state}")
    print()

    def show(factor: str):
        result = factor.ljust(10, " ") + display(stock[factor])
        if f"{factor}_pct" in stock.index:
            result = f"{result} ({round(stock[f'{factor}_pct'] * 100, 2)}%)"

        print(result)

    show("P")
    print()

    print("[VALUATION]")
    show("GP/Y")
    show("GP/P")
    show("O/Y")
    show("O/P")
    show("E/Y")
    show("E/P")
    show("EQ")
    show("EQ/P")
    print()

    print("[GROWTH]")
    show("GP_QoQ")
    show("O_QoQ")
    show("E_QoQ")
    print()


def main():
    stat("071840")


if __name__ == '__main__':
    main()
