from gold_transform.base_transformer import BaseTransformer


class FatoIdadeTransformer(BaseTransformer):
    def __init__(self):
        super().__init__()
        self.headers = [
            "idade_id",
            "clube_id",
            "tempo_id",
            "media_idade_time_titular",
            "media_idade"
        ]

    def get_data_type(self):
        return "fato_idade"


    def transform_data(self):
        df_age = self.get_data_from_silver("age")
        df_clube = self.get_data_from_gold("dim_clube")
        df_tempo = self.get_data_from_gold("dim_tempo")

        df = df_age.merge(df_clube, left_on='clube', right_on='nome', how='left').rename(columns={'clube_id': 'clube_id'})
        df = df.merge(df_tempo, on=['ano'], how='left').rename(columns={'tempo_id': 'tempo_id'})

        # Verificar se todos os valores de tempo_id foram preenchidos
        missing_tempo_ids = df[df['tempo_id'].isnull()]
        if not missing_tempo_ids.empty:
            raise ValueError("Existem valores nulos em tempo_id após o merge. Verifique o mapeamento de ano.")

        df['tempo_id'] = df['tempo_id'].astype(int)

        df['idade_id'] = df.index + 1
        return df[['idade_id', 'clube_id', 'tempo_id', 'media_idade_time_titular', 'media_idade']]

