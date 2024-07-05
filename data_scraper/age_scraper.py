# Classe AgeScraper para coletar dados de idade dos times

from .base_scraper import DataScraper
from bs4 import BeautifulSoup


class AgeScraper(DataScraper):
    def __init__(self, start_year, end_year, force_update_years):
        """
        Inicializa a classe AgeScraper.

        :param start_year: Ano inicial para a coleta de dados.
        :param end_year: Ano final para a coleta de dados.
        :param force_update_years: Lista de anos para os quais a atualização de dados deve ser forçada.
        """
        super().__init__(start_year, end_year, force_update_years)
        self.headers = ['ano', 'clube', 'plantel', 'jogadores_utilizados', 'media_idade_plantel',
                        'media_idade_time_titular', 'media_idade']

    def get_data_type(self):
        """
        Retorna o tipo de dados sendo coletados.

        :return: String indicando o tipo de dados ("age").
        """
        return "age"

    def get_data(self, year):
        """
        Coleta os dados de idade dos times para um ano específico.

        :param year: Ano para o qual os dados serão coletados.
        :return: Lista de dados coletados para o ano especificado.
        """
        # Monta a URL para o ano especificado
        url = f"https://www.transfermarkt.com.br/campeonato-brasileiro-serie-a/altersschnitt/wettbewerb/BRA1/saison_id/{year}/plus/1"

        # Faz a requisição dos dados e faz o parsing do conteúdo HTML
        content = self.fetch_data(url)
        soup = BeautifulSoup(content, 'html.parser')
        table = soup.find('table', class_='items')

        data = []
        if table:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) > 1:
                    team = cells[1].get_text(strip=True)
                    squad_size = cells[2].get_text(strip=True)
                    players_used = cells[3].get_text(strip=True)
                    avg_age_squad = self.convert_string_to_double(cells[4].get_text(strip=True))
                    avg_age_starting = self.convert_string_to_double(cells[5].get_text(strip=True))
                    avg_age = self.convert_string_to_double(cells[6].get_text(strip=True))
                    data.append([year, team, squad_size, players_used, avg_age_squad, avg_age_starting, avg_age])
        return data
