import pandas as pd
from connectors.air_quality_api import AirQualityApiClient
from pathlib import Path


def extract_air_quality(
    air_quality_api_client: AirQualityApiClient, city_reference_path: Path
) -> pd.DataFrame:
    """
    Perform extraction using a filepath which contains a list of cities.
    """
    df_cities = pd.read_csv(city_reference_path)
    aq_data = []
    for city in df_cities["city"]:
        city_parsed = city.replace(" ", "-").lower()
        aq_data.append(air_quality_api_client.get_air_quality(city_name=city_parsed))

    df_aq = pd.json_normalize(aq_data)
    return df_aq, df_cities
