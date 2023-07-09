from core.repository.maria.manage import *


def main():
    # 일봉 차트 일부 업데이트
    # 본 코드 실행전에 chart 테이블에서 종복될 데이터 미리 삭제 필요
    update_chart(date(2023, 6, 1))

    # 월봉 차트 일부 업데이트
    insert_month_chart("month_chart", year=2023, month=6)


if __name__ == '__main__':
    main()
