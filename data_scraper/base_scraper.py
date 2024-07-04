# Classe base DataScraper com a lógica comum para todos os scrapers

import os
import requests
import pandas as pd
import time
from .club_mapping import club_mapping


class DataScraper:
    def __init__(self, start_year, end_year, force_update_years, is_sleep_enable=False):
        """
        Inicializa a classe DataScraper.

        :param start_year: Ano inicial para a coleta de dados.
        :param end_year: Ano final para a coleta de dados.
        :param force_update_years: Lista de anos para os quais a atualização de dados deve ser forçada.
        :param is_sleep_enable: Habilita ou desabilita o atraso entre as solicitações para evitar bloqueios.
        """
        self.start_year = start_year
        self.end_year = end_year
        self.force_update_years = force_update_years
        self.headers = []
        self.is_sleep_enable = is_sleep_enable
        self.club_mapping = club_mapping

    def fetch_data(self, url):
        """
        Faz a requisição dos dados da URL fornecida.

        :param url: URL para buscar os dados.
        :return: Conteúdo da resposta da requisição.
        """
        print(f"Fetching data from {url}")
        response = requests.get(url, headers={
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'})
        response.raise_for_status()
        return response.content

    def save_data(self, data, file_path):
        """
        Salva os dados em um arquivo CSV.

        :param data: Dados a serem salvos.
        :param file_path: Caminho do arquivo para salvar os dados.
        """
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
        df = pd.DataFrame(data, columns=self.headers)
        df.to_csv(file_path, index=False)
        print(f"Data saved to {file_path}")

    def collect_and_save_data(self):
        """
        Coleta e salva os dados para todos os anos no intervalo especificado.
        """
        all_data = []
        for year in range(self.start_year, self.end_year + 1):
            file_path = f"../data/{self.get_data_type()}/{self.get_data_type()}_{year}.csv"
            if os.path.exists(file_path) and year not in self.force_update_years:
                existing_df = pd.read_csv(file_path)
                all_data.extend(existing_df.values.tolist())
            else:
                data = self.get_data(year)
                if data:
                    self.save_data(data, file_path)
                    all_data.extend(data)
                    if self.is_sleep_enable:
                        time.sleep(1)

    def convert_string_to_double(self, value):
        """
        Converte uma string que representa um valor numérico para float.

        :param value: String representando o valor numérico (ex: '25,7').
        :return: Valor numérico em float.
        """
        try:
            return float(value.replace(',', '.'))
        except ValueError:
            return None

    def get_data_type(self):
        """
        Retorna o tipo de dados sendo coletados.
        Este método deve ser implementado nas classes derivadas.
        """
        raise NotImplementedError("This method should be overridden in derived classes")

    def get_data(self, year):
        """
        Coleta os dados para um ano específico.
        Este método deve ser implementado nas classes derivadas.
        """
        raise NotImplementedError("This method should be overridden in derived classes")

    def normalize_club_name(self, club_name):
        """
        Normaliza o nome do clube usando o mapeamento fornecido.

        :param club_name: Nome do clube a ser normalizado.
        :return: Nome normalizado do clube.
        """
        return self.club_mapping.get(club_name, club_name)
