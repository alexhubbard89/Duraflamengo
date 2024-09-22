from fmp.minute_by_minute import collect_mxm_price, migrate_data
import datetime as dt


def main():
    print("Staring Minute collection")
    while dt.datetime.now().time() < dt.time(20, 1):
        ds = dt.date.today()
        print("\n\n\ncollecting...\n\n\n")
        collect_mxm_price(ds)
        print("\n\n\nmigrating...\n\n\n")
        migrate_data(ds)
    print("Terminating Minute collection")


if __name__ == "__main__":
    main()
