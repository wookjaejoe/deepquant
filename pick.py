from backtest import GppaBackTest
from base import YearMonth


def main():
    today = YearMonth.today()
    df = GppaBackTest(
        from_ym=today,
        to_ym=today,
        portfolio_size=10
    ).pick(ym=YearMonth.today())
    print(df[:100].to_csv())


if __name__ == '__main__':
    main()
