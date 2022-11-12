import pandas

from core import GPPA
from repository.maria.corp import get_name

pandas.set_option('display.max_columns', None)


def main():
    df = GPPA.calc()
    df['name'] = [get_name(code) for code in df.index]
    df[:30].to_csv(".out/pick.csv")


if __name__ == '__main__':
    main()
