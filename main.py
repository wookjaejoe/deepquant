from backtest import GppaBackTest
from base import YearMonth


def main():
    GppaBackTest(
        from_ym=YearMonth(2006, 3),
        to_ym=YearMonth.today().pre().pre(),
        pw=1.9,
        aw=0.9,
        portfolio_size=10
    ).run()


if __name__ == '__main__':
    main()
