from gold_transform.base_transformer import BaseTransformer


class FatoDesempenhoTransformer(BaseTransformer):
    def __init__(self):
        super().__init__()
        self.headers = [
            "desempenho_id",
            "clube_id",
            "tempo_id",
            "jogos",
            "vitorias",
            "empates",
            "derrotas",
            "gols_pro",
            "gols_contra",
            "saldo",
            "pontos"
        ]

    def get_data_type(self):
        return "fato_desempenho"

    def transform_data(self):
        df_round = self.get_data_from_silver("round")
        df_clube = self.get_data_from_gold("dim_clube")
        df_tempo = self.get_data_from_gold("dim_tempo")

        df = df_round.merge(df_clube, left_on='clube', right_on='nome', how='left').rename(
            columns={'clube_id': 'clube_id'})
        df = df.merge(df_tempo, on=['ano', 'rodada'], how='left').rename(columns={'tempo_id': 'tempo_id'})

        df['desempenho_id'] = df.index + 1
        return df[['desempenho_id', 'clube_id', 'tempo_id', 'jogos', 'vitorias', 'empates', 'derrotas', 'gols_pro',
                   'gols_contra', 'saldo', 'pontos']]
