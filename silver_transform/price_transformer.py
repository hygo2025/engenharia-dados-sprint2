from silver_transform.base_transformer import SilverTransform
from utils.club_mapping import normalize_club_name
from utils.utils import Utils


class PriceTransform(SilverTransform):
    def __init__(self):
        super().__init__()
        self.headers = ['ano', 'clube', 'valor_mercado_euros']

    def get_data_type(self):
        return "price"

    def transform_data(self):
        df = self.get_data()

        # Dropando colunas que nao serao usadas
        df = df.drop(columns=['plantel', 'media_idade', 'estrangeiros', 'media_valor_mercado'])

        # Normalizar o nome dos clubes
        df['clube'] = df['clube'].apply(normalize_club_name)

        # Normalizar o valor de mercado total
        df['valor_mercado_euros'] = df['valor_mercado_total'].apply(Utils.convert_price)

        # Dropando coluna valor_mercado_total
        df = df.drop(columns=['valor_mercado_total'])

        # Forçar os tipos das colunas
        df['ano'] = df['ano'].astype(int)
        df['valor_mercado_euros'] = df['valor_mercado_euros'].astype(float)
        return df
