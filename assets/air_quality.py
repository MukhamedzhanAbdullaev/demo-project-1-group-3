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


def transform(df_aq: pd.DataFrame, df_cities: pd.DataFrame) -> pd.DataFrame:
    """Transform the raw dataframes."""
    pd.options.mode.chained_assignment = None  # default='warn'
    
    # set city names to same format
    df_cities["city_name"] = df_cities["city"].str.lower().replace(r'[\s-]', '', regex=True)
    df_aq["city_name"] = df_aq["city_name"].str.lower().replace(r'[\s-]', '', regex=True)

    df_merged = pd.merge(left=df_cities, right=df_aq, left_on=["city_name"], right_on=["city_name"])

    # select specific columns 
    df_selected = df_merged[["city", "country", "aqi", "city_population", "city_area_km2", "iso"]]
   
    df_selected["aqi"] = pd.to_numeric(df_selected["aqi"], errors='coerce')
    df_selected["aqi"] = df_selected["aqi"].astype('Int64')
   
    # add ratio city_area_km2/aqi
    df_selected["ratio"] = (df_selected["city_area_km2"]/df_selected["aqi"]).round(2)     #pd.to_numeric(df_selected["aqi"], errors='coerce')

    # add rank by population 
    df_selected["population_rank"] = df_selected["city_population"].rank(ascending=False).astype(int)

    # add population/area column
    df_selected["population_density"] = (df_selected["city_population"]/df_selected["city_area_km2"]).astype(int)

    df_selected = df_selected.rename(
        columns={"aqi": "air_quality_index", "iso": "update_datetime"}
    )

    return df_selected
