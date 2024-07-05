# Script principal para execução dos scrapers

from data_scraper.age_scraper import AgeScraper
from data_scraper.price_scraper import PriceScraper
from data_scraper.round_scraper import RoundScraper
from data_scraper.home_away_scraper import HomeAwayScraper
from utils.utils import Utils
from databricks.connect import DatabricksSession
from databricks.sdk.core import Config


def main():
    rounds_per_season = {2024: 13}
    start_year = 2006
    end_year = 2024
    force_update_years = []
    input_folder = "data"
    bucket = "databricks-workspace-stack-4f961-bucket"
    base_path = "unity-catalog/2370156648242123"


    # # Coleta e salva dados de idade
    # age_scraper = AgeScraper(start_year, end_year, force_update_years)
    # age_scraper.collect_and_save_data()
    #
    # # Coleta e salva dados de preço
    # price_scraper = PriceScraper(start_year, end_year, force_update_years)
    # price_scraper.collect_and_save_data()
    #
    # # Coleta e salva dados de rodadas
    # round_scraper = RoundScraper(start_year, end_year, force_update_years, rounds_per_season)
    # round_scraper.collect_and_save_data()
    #
    # # Coleta e salva dados de jogos dentro e fora
    # home_away_scraper = HomeAwayScraper(start_year, end_year, force_update_years, rounds_per_season)
    # home_away_scraper.collect_and_save_data()

    # Utils.upload_all_csv_files(input_folder, bucket, base_path, folder_name="age")
    # Utils.upload_all_csv_files(input_folder, bucket, base_path, folder_name="price")
    # Utils.upload_all_csv_files(input_folder, bucket, base_path, folder_name="round")
    # Utils.upload_all_csv_files(input_folder, bucket, base_path, folder_name="home_away")

    config = Config(
        cluster_id='0704-210611-1z4xoxac'
    )

    spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()

    df = spark.read.table("2025_hygo.bronze.age")
    df.show(5)

if __name__ == "__main__":
    main()
