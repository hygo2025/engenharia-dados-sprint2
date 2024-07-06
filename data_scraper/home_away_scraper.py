# Classe HomeAwayScraper para coletar dados de jogos dentro e fora

import re

from bs4 import BeautifulSoup

from .base_scraper import DataScraper


class HomeAwayScraper(DataScraper):
    def __init__(self, start_year, end_year, force_update_years, rounds_per_season):
        """
        Inicializa a classe HomeAwayScraper.

        :param start_year: Ano inicial para a coleta de dados.
        :param end_year: Ano final para a coleta de dados.
        :param force_update_years: Lista de anos para os quais a atualização de dados deve ser forçada.
        :param rounds_per_season: Dicionário com o número de rodadas por temporada.
        """
        super().__init__(start_year, end_year, force_update_years)
        self.rounds_per_season = rounds_per_season
        self.headers = ['ano', 'rodada', 'clube_mandante', 'clube_visitante', 'gols_mandante', 'gols_visitante', 'data',
                        'juiz', 'publico']

    def get_data_type(self):
        """
        Retorna o tipo de dados sendo coletados.

        :return: String indicando o tipo de dados ("home_away").
        """
        return "home_away"

    def get_data(self, year):
        """
        Coleta os dados de jogos dentro e fora para um ano específico.

        :param year: Ano para o qual os dados serão coletados.
        :return: Lista de dados coletados para o ano especificado.
        """
        num_rounds = self.rounds_per_season.get(year, 38)
        data = []
        for round_number in range(1, num_rounds + 1):
            # Monta a URL para o ano e rodada especificados
            url = f"https://www.transfermarkt.com.br/campeonato-brasileiro-serie-a/spieltag/wettbewerb/BRA1/plus/?saison_id={year - 1}&spieltag={round_number}"

            # Faz a requisição dos dados e faz o parsing do conteúdo HTML
            content = self.fetch_data(url)
            soup = BeautifulSoup(content, 'html.parser')
            tables = soup.find_all('table')

            for table in tables:
                counter = 0
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) == 1:
                        anchor = cells[0].find('a')
                        if anchor is not None:
                            if counter == 0:
                                data[len(data) - 1].append(anchor.get_text(strip=True))
                            if counter == 1:
                                data[len(data) - 1].append(anchor.get_text(strip=True))
                                try:
                                    public = cells[0].get_text(strip=True).split(' ')[0]
                                    data[len(data) - 1].append(public)
                                except Exception:
                                    data[len(data) - 1].append('')
                            counter = counter + 1
                    if len(cells) == 9:
                        # Normaliza os nomes dos clubes e coleta os dados relevantes
                        home_team = self.extract_club_name(cells[0].get_text(strip=True))
                        score = cells[4].get_text(strip=True)
                        away_team = self.extract_club_name(cells[7].get_text(strip=True))
                        try:
                            home_score, away_score = map(int, score.split(':'))
                        except ValueError:
                            home_score, away_score = 'adiado', 'adiado'
                        data.append([year, round_number, home_team, away_team, home_score, away_score])
        return data

    @staticmethod
    def extract_club_name(raw_name):
        """
        Extrai o nome do clube removendo números e parênteses.

        :param raw_name: Nome cru do clube contendo números e parênteses.
        :return: Nome do clube sem números e parênteses.
        """
        return re.sub(r'\(\d+\.\)', '', raw_name).strip()
