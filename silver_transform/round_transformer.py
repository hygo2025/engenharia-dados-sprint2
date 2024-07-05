from silver_transform.base_transformer import SilverTransform
from utils.club_mapping import normalize_club_name


class RoundTransform(SilverTransform):
    def __init__(self):
        super().__init__()
        self.headers = ['ano', 'rodada', 'classificacao', 'clube', 'jogos', 'vitorias', 'empates', 'derrotas',
                        'gols_pro', 'gols_contra', 'saldo', 'pontos']

    def get_data_type(self):
        return "round"

    def transform_data(self):
        df = self.get_data()

        # Normalizar o nome dos clubes
        df['clube'] = df['clube'].apply(normalize_club_name)

        # Forçar os tipos das colunas
        df['ano'] = df['ano'].astype(int)
        df['rodada'] = df['rodada'].astype(int)
        df['jogos'] = df['jogos'].astype(int)
        df['vitorias'] = df['vitorias'].astype(int)
        df['empates'] = df['empates'].astype(int)
        df['derrotas'] = df['derrotas'].astype(int)
        df['gols_pro'] = df['gols_pro'].astype(int)
        df['gols_contra'] = df['gols_contra'].astype(int)
        df['saldo'] = df['saldo'].astype(int)
        df['pontos'] = df['pontos'].astype(int)

        return df
