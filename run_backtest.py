from datetime import date

from backtest import BackTest
from core import Gppa


def main():
    BackTest(
        factor=Gppa,
        from_date=date(2001, 4, 1),
        to_date=date(2022, 10, 31),
        portfolio_size=10
    ).run()


if __name__ == '__main__':
    main()
