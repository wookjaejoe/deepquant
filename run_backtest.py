from datetime import date

from backtest import BackTest
from core import GPPA


def main():
    BackTest(
        factor=GPPA,
        from_date=date(2001, 4, 1),
        to_date=date(2022, 10, 31),
        portfolio_size=10
    ).run()


if __name__ == '__main__':
    main()
