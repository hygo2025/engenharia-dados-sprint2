# Classe PriceScraper para coletar dados de preço dos times

from bs4 import BeautifulSoup

from .base_scraper import DataScraper


class PriceScraper(DataScraper):
    def __init__(self, start_year, end_year, force_update_years):
        """
        Inicializa a classe PriceScraper.

        :param start_year: Ano inicial para a coleta de dados.
        :param end_year: Ano final para a coleta de dados.
        :param force_update_years: Lista de anos para os quais a atualização de dados deve ser forçada.
        """
        super().__init__(start_year, end_year, force_update_years)
        self.headers = ['ano', 'clube', 'plantel', 'media_idade', 'estrangeiros', 'media_valor_mercado',
                        'valor_mercado_total']

    def get_data_type(self):
        """
        Retorna o tipo de dados sendo coletados.

        :return: String indicando o tipo de dados ("price").
        """
        return "price"

    def get_data(self, year):
        """
        Coleta os dados de preço dos times para um ano específico.

        :param year: Ano para o qual os dados serão coletados.
        :return: Lista de dados coletados para o ano especificado.
        """
        # Monta a URL para o ano especificado
        url = f"https://www.transfermarkt.com.br/campeonato-brasileiro-serie-a/startseite/wettbewerb/BRA1/plus/?saison_id={year}"

        # Faz a requisição dos dados e faz o parsing do conteúdo HTML
        content = self.fetch_data(url)
        soup = BeautifulSoup(content, 'html.parser')
        table = soup.find('table', class_='items')

        data = []
        if table:
            rows = table.find_all('tr', class_=['odd', 'even'])
            for row in rows:
                cells = row.find_all('td')
                if len(cells) > 1:
                    # Normaliza o nome do clube e coleta os dados relevantes
                    team = cells[1].get_text(strip=True)
                    squad_size = cells[2].get_text(strip=True)
                    avg_age = cells[3].get_text(strip=True)
                    foreigners = cells[4].get_text(strip=True)
                    avg_market_value = cells[5].get_text(strip=True)
                    total_market_value = cells[6].get_text(strip=True)
                    data.append([year, team, squad_size, avg_age, foreigners, avg_market_value, total_market_value])
        return data
