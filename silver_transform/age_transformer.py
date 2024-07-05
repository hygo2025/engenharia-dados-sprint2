from silver_transform.base_transformer import SilverTransform
from utils.club_mapping import normalize_club_name


class AgeTransform(SilverTransform):
    def __init__(self):
        super().__init__()
        self.headers = ['ano', 'clube', 'media_idade_time_titular', 'media_idade']

    def get_data_type(self):
        return "age"

    def transform_data(self):
        df = self.get_data()
        # Dropando colunas que nao serao usadas
        df = df.drop(columns=['plantel', 'jogadores_utilizados', 'media_idade_plantel'])

        # Normalizar o nome dos clubes
        df['clube'] = df['clube'].apply(normalize_club_name)

        # Forçar os tipos das colunas
        df['ano'] = df['ano'].astype(int)
        df['media_idade_time_titular'] = df['media_idade_time_titular'].astype(float)
        df['media_idade'] = df['media_idade'].astype(float)
        return df
