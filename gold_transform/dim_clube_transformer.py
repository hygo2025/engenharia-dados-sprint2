import pandas as pd

from base_transformer import BaseTransformer


class DimClubeTransformer(BaseTransformer):
    def __init__(self):
        super().__init__()
        self.headers = [
            "clube_id",
            "nome"
        ]

    def get_data_type(self):
        return "dim_clube"

    def transform_data(self):
        df_age = self.get_data_from_silver("age")
        df_home_away = self.get_data_from_silver("home_away")
        df_price = self.get_data_from_silver("price")
        df_round = self.get_data_from_silver("round")

        # Extração de clubes únicos
        clubes = pd.concat(
            [df_age['clube'], df_home_away['clube_mandante'], df_home_away['clube_visitante'], df_price['clube'],
             df_round['clube']])
        clubes = clubes.unique()
        clubes_df = pd.DataFrame(clubes, columns=['nome'])
        clubes_df['clube_id'] = clubes_df.index + 1
        return clubes_df[['clube_id', 'nome']]
