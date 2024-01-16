from dotenv import load_dotenv
import os
from connectors.air_quality_api import AirQualityApiClient
from connectors.posgresql import PostgreSqlClient
from sqlalchemy import Table, MetaData, Column, Integer, String, Float
from assets.air_quality import (
    extract_air_quality,
    transform,
    load
)
import yaml
from pathlib import Path
import schedule
import time

if __name__ == "__main__":
    load_dotenv()
    # LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER_NAME")
    # LOGGING_DATABASE_NAME = os.environ.get("LOGGING_DATABASE_NAME")
    # LOGGING_USERNAME = os.environ.get("LOGGING_USERNAME")
    # LOGGING_PASSWORD = os.environ.get("LOGGING_PASSWORD")
    # LOGGING_PORT = os.environ.get("LOGGING_PORT")
    API_KEY = os.environ.get("API_KEY")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("PORT")

    # get config variables
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
            PIPELINE_NAME = pipeline_config.get("name")
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )
    
    air_quality_api_client = AirQualityApiClient(api_key=API_KEY)

    config=pipeline_config.get("config")

    #extract
    df_aq, df_cities = extract_air_quality(
        air_quality_api_client=air_quality_api_client,
        city_reference_path=config.get("city_reference_path"),
    )

    #transform
    df_transformed = transform(df_aq=df_aq, df_cities=df_cities)
    print(df_transformed.head())

    #load
    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )
    metadata = MetaData()
    table = Table(
        "air_quality_city_data",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("iso_datetime", String),
        Column("city", String),
        Column("country", String),
        Column("population", Integer),
        Column("temperature", Float),
        Column("humidity", Float),
        Column("pm10", Float),
        Column("pm2.5", Float),
        Column("population/km2", Integer),
        Column("aqi", Integer),
        Column("aqi_rank", Integer),
        Column("air_pollution_level", String),
    )
    load(
        df=df_transformed,
        postgresql_client=postgresql_client,
        table=table,
        metadata=metadata,
        load_method="upsert",
    )