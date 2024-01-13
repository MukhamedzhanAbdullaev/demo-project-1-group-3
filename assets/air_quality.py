import pandas as pd
from connectors.air_quality_api import AirQualityApiClient
from pathlib import Path


def extract_aqi(
    air_quality_api_client: AirQualityApiClient, city_reference_path: Path
) -> pd.DataFrame:
    """
    Perform extraction using a filepath which contains a list of cities.
    """
    df_cities = pd.read_csv(city_reference_path)
    aqi_data = []
    for city in df_cities["city"]:
        city_parsed = city.replace(" ", "-").lower()
        aqi_data.append(air_quality_api_client.get_city(city_name=city_parsed))

    df_aqi = pd.json_normalize(aqi_data)
    print(df_aqi.head())
    return df_aqi
