from backtest import GppaBackTest
from base import YearMonth


def main():
    GppaBackTest(
        from_ym=YearMonth(2001, 4),
        to_ym=YearMonth.today().pre().pre(),
    ).run()


if __name__ == '__main__':
    main()
