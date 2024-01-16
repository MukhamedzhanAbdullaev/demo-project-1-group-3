import pandas as pd
from connectors.air_quality_api import AirQualityApiClient
from connectors.posgresql import PostgreSqlClient
from pathlib import Path
from sqlalchemy import Table, MetaData, Column, Integer, String, Float


def extract_air_quality(
    air_quality_api_client: AirQualityApiClient, city_reference_path: Path
) -> pd.DataFrame:
    """
    Perform extraction using a filepath which contains a list of cities and API which has live data on each city.
    """
    df_cities = pd.read_csv(city_reference_path)
    aq_data = []
    for city in df_cities["city"]:
        city_parsed = city.replace(" ", "-").lower()
        aq_data.append(air_quality_api_client.get_air_quality(city_name=city_parsed))

    df_aq = pd.json_normalize(aq_data)
    # print(df_aq.iloc[:, : 10])
    return df_aq, df_cities


def transform(df_aq: pd.DataFrame, df_cities: pd.DataFrame) -> pd.DataFrame:
    """Transform the raw dataframes."""
    pd.options.mode.chained_assignment = None  # default='warn'
    
    # set city names to same format
    df_cities["city_name"] = df_cities["city"].str.lower().replace(r'[\s-]', '', regex=True)
    df_aq["city_name"] = df_aq["city_name"].str.lower().replace(r'[\s-]', '', regex=True)

    df_merged = pd.merge(left=df_cities, right=df_aq, left_on=["city_name"], right_on=["city_name"])

    # select specific columns 
    df_selected = df_merged[["city", "country", "city_population", "city_area_km2", "aqi", "h.v", "t.v", "pm10.v", "pm25.v", "iso"]]
   
    df_selected["aqi"] = pd.to_numeric(df_selected["aqi"], errors='coerce')
    df_selected["aqi"] = df_selected["aqi"].astype('Int64')

    # add rank by aqi (higher reanked is better)
    df_selected["aqi_rank"] = df_selected["aqi"].rank(ascending=True).astype(int)

    # add population density column
    df_selected["population/km2"] = (df_selected["city_population"]/df_selected["city_area_km2"]).astype(int)

    #drop area column
    df_selected = df_selected.drop(columns=['city_area_km2'])

    #Rename columns
    df_selected = df_selected.rename(
        columns={"city_population": "population", "h.v": "humidity", "t.v": "temperature", "pm10.v": "pm10", "pm25.v": "pm2.5", "iso": "iso_datetime"}
    )

    #Add category rank depending on value of aqi
    df_selected['air_pollution_level'] = pd.cut(df_selected['aqi'], [0, 50, 100, 150, 200, 300, 1000], labels = ['Good', 'Moderate', 'Unhealthy if Sensitive', 'Unhealthy', 'Very Unhealthy', 'Hazardous'])

    return df_selected

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
        df: dataframe to load
        postgresql_client: postgresql client
        table: sqlalchemy table
        metadata: sqlalchemy metadata
        load_method: supports one of: [insert, upsert, overwrite]
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