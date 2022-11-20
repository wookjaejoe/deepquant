from datetime import date

from backtest import BackTest


def main():
    BackTest(
        begin=date(2001, 4, 30),
        end=date(2022, 10, 31),
        port_size=10
    ).run()


if __name__ == '__main__':
    main()
