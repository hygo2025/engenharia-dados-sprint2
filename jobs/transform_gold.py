from gold_transform.dim_clube_transformer import DimClubeTransformer
from gold_transform.dim_tempo_transformer import DimTempoTransformer
from gold_transform.fato_desempenho_transformer import FatoDesempenhoTransformer
from gold_transform.fato_idade_transformer import FatoIdadeTransformer
from gold_transform.fato_jogos_transformer import FatoJogosTransformer
from gold_transform.fato_valor_mercado_transformer import FatoValorMercadoTransformer


def transform_gold():
    dim_clube_transform = DimClubeTransformer()
    dim_clube_transform.transform()

    dim_tempo_transformer = DimTempoTransformer()
    dim_tempo_transformer.transform()

    fato_jogos_transformer = FatoJogosTransformer()
    fato_jogos_transformer.transform()

    fato_desempenho_transformer = FatoDesempenhoTransformer()
    fato_desempenho_transformer.transform()

    fato_valor_mercado_transformer = FatoValorMercadoTransformer()
    fato_valor_mercado_transformer.transform()

    fato_idade_transformer = FatoIdadeTransformer()
    fato_idade_transformer.transform()
