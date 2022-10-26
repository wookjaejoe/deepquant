import repository.deepsearch as ds
import repository.maria.chart as chart


def main():
    title = '매출액'
    x1 = ds.load_by_year(title, 2020)
    x2 = list(ds.load_all_by(title, 2020, 4, 4))
    print()

    # titles = ['매출액', '매출원가', '자산', '자본', '당기순이익']
    # for year in range(2005, 2023):
    #     for q in [1, 2, 3, 4]:
    #         for title in titles:
    #             try:
    #                 ds.collect_by_quart(title, year, q)
    #                 print(title, year, q, len(ds.load_by_quart(title, year, q).index))
    #             except Exception as e:
    #                 print(f'No file for {title, year, q}')
    #
    # title = '매출액'
    # x = ds.load_by_year(title, 2013)
    # y = sum(ds.load_all_by(title, 2013, 4, 4))
    # print()
    # 2022년도까지 사용가능


# upload_all_corp_from_pre_queried()
if __name__ == '__main__':
    main()
