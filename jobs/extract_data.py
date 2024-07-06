# Script principal para execução dos scrapers

from data_scraper.age_scraper import AgeScraper
from data_scraper.home_away_scraper import HomeAwayScraper
from data_scraper.price_scraper import PriceScraper
from data_scraper.round_scraper import RoundScraper


def main():
    rounds_per_season = {2024: 14}
    start_year = 2006
    end_year = 2024
    force_update_years = []

    # Coleta e salva dados de idade
    age_scraper = AgeScraper(start_year, end_year, force_update_years)
    age_scraper.collect_and_save_data()

    # Coleta e salva dados de preço
    price_scraper = PriceScraper(start_year, end_year, force_update_years)
    price_scraper.collect_and_save_data()

    # Coleta e salva dados de rodadas
    round_scraper = RoundScraper(start_year, end_year, force_update_years, rounds_per_season)
    round_scraper.collect_and_save_data()

    # Coleta e salva dados de jogos dentro e fora
    home_away_scraper = HomeAwayScraper(start_year, end_year, force_update_years, rounds_per_season)
    home_away_scraper.collect_and_save_data()


if __name__ == "__main__":
    main()
