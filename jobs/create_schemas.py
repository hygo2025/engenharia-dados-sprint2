from utils.spark_session import SparkSession


def main():
    spark = SparkSession().get_spark()

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS gold")

    # Verificar se o schema foi criado
    schemas = spark.sql("SHOW SCHEMAS")
    schemas.show()


if __name__ == "__main__":
    main()
