# Classe RoundScraper para coletar dados de rodadas

from bs4 import BeautifulSoup

from .base_scraper import DataScraper


class RoundScraper(DataScraper):
    def __init__(self, start_year, end_year, force_update_years, rounds_per_season):
        """
        Inicializa a classe RoundScraper.

        :param start_year: Ano inicial para a coleta de dados.
        :param end_year: Ano final para a coleta de dados.
        :param force_update_years: Lista de anos para os quais a atualização de dados deve ser forçada.
        :param rounds_per_season: Dicionário com o número de rodadas por temporada.
        """
        super().__init__(start_year, end_year, force_update_years, 'rounds')
        self.rounds_per_season = rounds_per_season
        self.headers = ['ano', 'rodada', 'classificacao', 'clube', 'jogos', 'vitorias', 'empates', 'derrotas',
                        'gols_pro', 'gols_contra', 'saldo', 'pontos']

    def get_data_type(self):
        """
        Retorna o tipo de dados sendo coletados.

        :return: String indicando o tipo de dados ("round").
        """
        return "round"

    def get_data(self, year):
        """
        Coleta os dados de rodadas para um ano específico.

        :param year: Ano para o qual os dados serão coletados.
        :return: Lista de dados coletados para o ano especificado.
        """
        num_rounds = self.rounds_per_season.get(year, 38)
        data = []
        for round_number in range(1, num_rounds + 1):
            # Monta a URL para o ano e rodada especificados
            url = f"https://www.transfermarkt.com.br/campeonato-brasileiro-serie-a/spieltagtabelle/wettbewerb/BRA1?saison_id={year - 1}&spieltag={round_number}"

            # Faz a requisição dos dados e faz o parsing do conteúdo HTML
            content = self.fetch_data(url)
            soup = BeautifulSoup(content, 'html.parser')
            table = soup.find('table', class_='items')

            if table:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if cells:
                        # Coleta os valores das células e processa os dados relevantes
                        cell_values = [cell.get_text(strip=True) for cell in cells if cell.get_text(strip=True)]
                        if len(cell_values) > 7:
                            gols_pro, gols_contra = cell_values[6].split(':')
                            cell_values = cell_values[:6] + [gols_pro, gols_contra] + cell_values[7:]
                        cell_values[3] = cell_values[3]
                        data.append([year, round_number] + cell_values)
        return data
