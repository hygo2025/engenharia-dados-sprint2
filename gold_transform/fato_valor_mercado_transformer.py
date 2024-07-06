from gold_transform.base_transformer import BaseTransformer


class FatoValorMercadoTransformer(BaseTransformer):
    def __init__(self):
        super().__init__()
        self.headers = [
            "valor_mercado_id",
            "clube_id",
            "tempo_id",
            "valor_mercado_euros"
        ]

    def get_data_type(self):
        return "fato_valor_mercado"

    def transform_data(self):
        df_price = self.get_data_from_silver("price")
        df_clube = self.get_data_from_gold("dim_clube")
        df_tempo = self.get_data_from_gold("dim_tempo")

        df = df_price.merge(df_clube, left_on='clube', right_on='nome', how='left').rename(
            columns={'clube_id': 'clube_id'})
        df = df.merge(df_tempo, on=['ano'], how='left').rename(columns={'tempo_id': 'tempo_id'})
        df['valor_mercado_id'] = df.index + 1
        return df[['valor_mercado_id', 'clube_id', 'tempo_id', 'valor_mercado_euros']]
