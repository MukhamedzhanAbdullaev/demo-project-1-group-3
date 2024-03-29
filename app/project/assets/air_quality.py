import pandas as pd
from project.connectors.air_quality_api import AirQualityApiClient
from project.connectors.postgresql import PostgreSqlClient
from pathlib import Path
from sqlalchemy import Table, MetaData, Column, Integer, String, Float

def extract_cities_data(city_reference_path: Path)->pd.DataFrame:
    """
    Perform extraction using a CSV file to get data on most populated cities in the world.
    
    Args:
    - city_reference_path (Path): The path to the CSV file containing city data.

    Returns:
    pd.DataFrame: A DataFrame containing information about the most populated cities.
    """
    df_cities = pd.read_csv(city_reference_path)
    return df_cities

def extract_air_quality(
    air_quality_api_client: AirQualityApiClient,
    df_cities: pd.DataFrame
) -> pd.DataFrame:
    """
    Perform extraction using an API which has live data on each city.
    
    Args:
    - air_quality_api_client (AirQualityApiClient): An instance of the AirQualityApiClient used to fetch air quality data.
    - df_cities (pd.DataFrame): A DataFrame containing information about cities for which air quality data is to be extracted.

    Returns:
    pd.DataFrame: A DataFrame containing air quality data for the specified cities.
    """
    aq_data = []
    for city in df_cities["city"]:
        aq_data.append(air_quality_api_client.get_air_quality(city=city))

    df_aq = pd.json_normalize(aq_data)
    return df_aq


def transform(df_aq: pd.DataFrame, df_cities: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the raw dataframes.
    
    Args:
    - df_aq (pd.DataFrame): DataFrame containing raw air quality data.
    - df_cities (pd.DataFrame): DataFrame containing information about cities.

    Returns:
    pd.DataFrame: Transformed DataFrame containing processed air quality and city information.
    """
    pd.options.mode.chained_assignment = None  # default='warn'
    
    # set city names to same format
    df_cities["city_name"] = df_cities["city"].str.lower().replace(r'[\s-]', '', regex=True)
    df_aq["city_name"] = df_aq["city_name"].str.lower().replace(r'[\s-]', '', regex=True)
    # merge dataframes
    df_merged = pd.merge(left=df_cities, right=df_aq, left_on=["city_name"], right_on=["city_name"])

    # select specific columns 
    df_aq_cities = df_merged[["city", "country", "city_population", "city_area_km2", "aqi", "h.v", "t.v", "pm10.v", "pm25.v", "iso"]]
    df_aq_cities["aqi"] = pd.to_numeric(df_aq_cities["aqi"], errors='coerce')
    df_aq_cities["aqi"] = df_aq_cities["aqi"].astype('int64')

    # add rank by aqi (higher ranked is better)
    df_aq_cities["aqi_rank"] = df_aq_cities["aqi"].rank(ascending=True).astype(int)

    # add population density column
    df_aq_cities["population/km2"] = (df_aq_cities["city_population"]/df_aq_cities["city_area_km2"]).astype(int)

    # drop area column
    df_aq_cities = df_aq_cities.drop(columns=['city_area_km2'])

    # rename columns
    df_aq_cities = df_aq_cities.rename(
        columns={"city_population": "population", "h.v": "humidity", "t.v": "temperature", "pm10.v": "pm10", "pm25.v": "pm2.5", "iso": "iso_datetime"}
    )

    #Add category rank depending on value of aqi
    df_aq_cities['air_pollution_level'] = pd.cut(df_aq_cities['aqi'], [0, 50, 100, 150, 200, 300, 1000], labels = ['Good', 'Moderate', 'Unhealthy if Sensitive', 'Unhealthy', 'Very Unhealthy', 'Hazardous'])
    df_aq_cities['air_pollution_level'] = df_aq_cities['air_pollution_level'].astype('object')

    return df_aq_cities

def load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method: str = "overwrite",
) -> None:
    """
    Load dataframe to either a database.

    Args:
    - df: Dataframe to load
    - postgresql_client: Postgresql client
    - table: Sqlalchemy table
    - metadata: Sqlalchemy metadata
    - load_method: Supports one of: [insert, upsert, overwrite]

    Raises:
    - Exception: If an invalid load method is specified.

    Returns:
    None
    """
    if load_method == "insert":
        postgresql_client.insert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "upsert":
        postgresql_client.upsert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "overwrite":
        postgresql_client.overwrite(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    else:
        raise Exception(
            "Please specify a correct load method: [insert, upsert, overwrite]"
        )
