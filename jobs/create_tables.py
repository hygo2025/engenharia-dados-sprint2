from utils.spark_session import SparkSession


def main():
    spark = SparkSession().get_spark()
    schema_name = "bronze"

    tables = {
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


if __name__ == "__main__":
    main()
