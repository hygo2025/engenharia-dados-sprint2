from utils.spark_session import SparkSession


def create_tables(spark, schema_name, tables):
    for table_name, table_definition in tables.items():
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} {table_definition}
            USING DELTA
        """)

    # Verifica se as tabelas foram criadas
    for table_name in tables.keys():
        tables_info = spark.sql(f"SHOW TABLES IN {schema_name} LIKE '{table_name}'")
        tables_info.show()

    spark.sql(f"SHOW TABLES IN {schema_name}").show()


def main():
    spark = SparkSession().get_spark()

    schema_name_bronze = "bronze"
    tables_bronze = {
        "age": """
            (
                ano bigint,
                clube string,
                plantel bigint,
                jogadores_utilizados bigint,
                media_idade_plantel double,
                media_idade_time_titular double,
                media_idade double
            )
        """,
        "home_away": """
            (
                ano bigint,
                rodada bigint,
                clube_mandante string,
                clube_visitante string,
                gols_mandante string,
                gols_visitante string,
                data string,
                juiz string,
                publico string
            )
        """,
        "price": """
            (
                ano bigint,
                clube string,
                plantel string,
                media_idade string,
                estrangeiros string,
                media_valor_mercado string,
                valor_mercado_total string
            )
        """,
        "round": """
            (
                ano bigint,
                rodada bigint,
                classificacao string,
                clube string,
                jogos string,
                vitorias string,
                empates string,
                derrotas string,
                gols_pro string,
                gols_contra string,
                saldo string,
                pontos string
            )
        """
    }

    schema_name_silver = "silver"
    tables_silver = {
        "age": """
            (
                ano bigint,
                clube string,
                plantel bigint,
                media_idade_time_titular double,
                media_idade double
            )
        """,
        "home_away": """
            (
                ano bigint,
                rodada bigint,
                clube_mandante string,
                clube_visitante string,
                gols_mandante bigint,
                gols_visitante bigint
            )
        """,
        "price": """
            (
                ano bigint,
                clube string,
                valor_mercado_euros double
            )
        """,
        "round": """
            (
                ano bigint,
                rodada bigint,
                classificacao string,
                clube string,
                jogos bigint,
                vitorias bigint,
                empates bigint,
                derrotas bigint,
                gols_pro bigint,
                gols_contra bigint,
                saldo bigint,
                pontos bigint
            )
        """
    }

    create_tables(spark, schema_name_bronze, tables_bronze)
    create_tables(spark, schema_name_silver, tables_silver)


if __name__ == "__main__":
    main()
