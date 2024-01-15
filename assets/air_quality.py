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
    print(df_aq.head())
    return df_aq, df_cities


#transform
def transform(df_aq: pd.DataFrame, df_cities: pd.DataFrame) -> pd.DataFrame:
    """Transform the raw dataframes."""
    pd.options.mode.chained_assignment = None  # default='warn'
    # set city names to lowercase (First Transformation)
    df_aq["city_name"] = df_aq["City"].str.lower() 
    df_merged = pd.merge(left=df_cities, right=df_aq, on=["City", "Country"]) ########### I cant tell if Country is a field in df_aq
    # Select specific columns (Second Transformation)
    df_selected = df_merged[["dt", "id", "name", "main.temp", "population"]]
    #df_selected["unique_id"] = df_selected["dt"].astype(str) + df_selected["id"].astype(str)

    # convert unix timestamp column to datetime (Third Transformation)
    df_selected["dt"] = pd.to_datetime(df_selected["dt"], unit="s")
    # rename colum names to more meaningful names
    df_selected = df_selected.rename(
        columns={"dt": "datetime", "main.temp": "temperature"}
    )
    df_selected = df_selected.set_index(["unique_id"])
    return df_selected
