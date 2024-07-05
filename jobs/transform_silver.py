from silver_transform.age_transformer import AgeTransform
from silver_transform.home_away_transformer import HomeAwayTransform
from silver_transform.price_transformer import PriceTransform
from silver_transform.round_transformer import RoundTransform


def main():
    # age_transform = AgeTransform()
    # age_transform.transform()
    #
    # price_transform = PriceTransform()
    # price_transform.transform()

    # home_away_transform = HomeAwayTransform()
    # home_away_transform.transform()

    round_transform = RoundTransform()
    round_transform.transform()

if __name__ == "__main__":
    main()
