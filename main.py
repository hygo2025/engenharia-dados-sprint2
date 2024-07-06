from jobs.create_schemas import create_schemas
from jobs.create_tables import create_tables
from jobs.extract_data import extract_data
from jobs.transform_gold import transform_gold
from jobs.transform_silver import transform_silver


def main():
    create_schemas()
    create_tables()
    extract_data()
    transform_silver()
    transform_gold()


if __name__ == "__main__":
    main()
