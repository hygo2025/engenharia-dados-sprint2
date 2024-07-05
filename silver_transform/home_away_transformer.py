from silver_transform.base_transformer import SilverTransform
from utils.club_mapping import normalize_club_name


class HomeAwayTransform(SilverTransform):
    def __init__(self):
        super().__init__()
        self.headers = ['ano', 'rodada', 'clube_mandante', 'clube_visitante', 'gols_mandante', 'gols_visitante']

    def get_data_type(self):
        return "home_away"

    def transform_data(self):
        df = self.get_data()

        # Dropando colunas que nao serao usadas
        df = df.drop(columns=['data', 'juiz', 'publico'])

        # Normalizar o nome dos clubes
        df['clube_mandante'] = df['clube_mandante'].apply(normalize_club_name)
        df['clube_visitante'] = df['clube_visitante'].apply(normalize_club_name)

        # Forçar os tipos das colunas
        df['ano'] = df['ano'].astype(int)
        df['rodada'] = df['rodada'].astype(int)
        # Tratando os jogos adiados, definindo gols como 0
        df['gols_mandante'] = df['gols_mandante'].replace('adiado', 0).astype(int)
        df['gols_visitante'] = df['gols_visitante'].replace('adiado', 0).astype(int)

        return df
