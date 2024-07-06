from gold_transform.base_transformer import BaseTransformer


class DimTempoTransformer(BaseTransformer):
    def __init__(self):
        super().__init__()
        self.headers = [
            "tempo_id",
            "ano",
            "rodada"
        ]

    def get_data_type(self):
        return "dim_tempo"

    def transform_data(self):
        df_home_away = self.get_data_from_silver("home_away")
        df_tempo = df_home_away[['ano', 'rodada']].drop_duplicates()

        # Ordenar o DataFrame por ano e depois por rodada
        df_tempo = df_tempo.sort_values(by=['ano', 'rodada'])

        df_tempo['tempo_id'] = df_tempo.index + 1
        return df_tempo[['tempo_id', 'ano', 'rodada']]
