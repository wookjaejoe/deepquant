from datetime import date
from core.repository.maria.manage import update_chart


def main():
    update_chart(date(1997, 1, 1))


if __name__ == '__main__':
    main()
