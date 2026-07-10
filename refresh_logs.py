from etl.api_loader import refresh as refresh_api
from etl.cobrand_loader import refresh as refresh_cobrand


def main():

    print("=" * 70)
    print("TheFinpedia Warehouse Refresh")
    print("=" * 70)

    print("\nRefreshing API warehouse...\n")
    refresh_api()

    print("\nRefreshing Cobrand warehouse...\n")
    refresh_cobrand()

    print("\n" + "=" * 70)
    print("Warehouse refresh completed successfully.")
    print("=" * 70)


if __name__ == "__main__":
    main()