import repository.deepsearch as ds


def main():
    for year in range(1996, 2022):
        ds.collect_by_year('자기자본이익율', year)


# upload_all_corp_from_pre_queried()
if __name__ == '__main__':
    main()
