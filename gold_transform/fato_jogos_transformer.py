from gold_transform.base_transformer import BaseTransformer


class FatoJogosTransformer(BaseTransformer):
    def __init__(self):
        super().__init__()
        self.headers = [
            "jogo_id",
            "clube_mandante_id",
            "clube_visitante_id",
            "gols_mandante",
            "gols_visitante",
            "tempo_id"
        ]

    def get_data_type(self):
        return "fato_jogos"

    def transform_data(self):
        df_home_away = self.get_data_from_silver("home_away")
        df_clube = self.get_data_from_gold("dim_clube")
        df_tempo = self.get_data_from_gold("dim_tempo")

        df = df_home_away.merge(df_clube, left_on='clube_mandante', right_on='nome', how='left').rename(
            columns={'clube_id': 'clube_mandante_id'})
        df = df.merge(df_clube, left_on='clube_visitante', right_on='nome', how='left').rename(
            columns={'clube_id': 'clube_visitante_id'})
        df = df.merge(df_tempo, on=['ano', 'rodada'], how='left').rename(columns={'tempo_id': 'tempo_id'})
        df['jogo_id'] = df.index + 1
        return df[['jogo_id', 'clube_mandante_id', 'clube_visitante_id', 'gols_mandante', 'gols_visitante', 'tempo_id']]
