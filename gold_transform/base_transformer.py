from utils.spark_session import SparkSession


class BaseTransformer:
    def __init__(self):
        self.origin_schema_name = 'silver'
        self.target_schema_name = 'gold'
        self.headers = []
        self.spark = SparkSession().get_spark()

    def get_data_from_silver(self, table_name):
        query = f"SELECT * FROM {self.origin_schema_name}.{table_name}"
        spark_df = self.spark.sql(query)
        return spark_df.toPandas()

    def get_data_from_gold(self, table_name):
        query = f"SELECT * FROM {self.target_schema_name}.{table_name}"
        spark_df = self.spark.sql(query)
        return spark_df.toPandas()

    def transform(self):
        """
        Salva os dados transformados na camada gold.
        """
        table_name = f"{self.target_schema_name}.{self.get_data_type()}"
        df = self.transform_data()

        # Criação do DataFrame Spark com schema
        spark_df_transformed = self.spark.createDataFrame(df, schema=self.headers)

        # Salvar o DataFrame na tabela
        spark_df_transformed.write.mode("overwrite").format("delta").saveAsTable(table_name)

        # Verificar se os dados foram salvos na tabela
        result = self.spark.sql(f"SELECT * FROM {table_name}")
        result.show()

    def get_data_type(self):
        """
        Retorna o tipo de dados sendo coletados.
        Este método deve ser implementado nas classes derivadas.
        """
        raise NotImplementedError("This method should be overridden in derived classes")

    def transform_data(self):
        """
        Transforma os dados.
        Este método deve ser implementado nas classes derivadas.
        """
        raise NotImplementedError("This method should be overridden in derived classes")
