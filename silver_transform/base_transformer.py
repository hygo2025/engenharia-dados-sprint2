import os
import requests
import pandas as pd
import time

from utils.spark_session import SparkSession


class SilverTransform:
    def __init__(self):
        self.origin_schema_name = 'bronze'
        self.target_schema_name = 'silver'
        self.headers = []
        self.spark = SparkSession().get_spark()

    def get_data(self):
        """
        Retorna os dados da camada bronze.
        """
        table_name = f"{self.origin_schema_name}.{self.get_data_type()}"
        query = f"SELECT * FROM {table_name}"
        spark_df = self.spark.sql(query)
        return spark_df.toPandas()

    def transform(self):
        """
        Salva os dados transformados na camada silver.
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
