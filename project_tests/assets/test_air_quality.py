import os
from pathlib import Path
from assets.air_quality import extract_cities_data, extract_air_quality, transform
from connectors.air_quality_api import AirQualityApiClient
import pytest
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import Table, MetaData, Column, String, Integer
from connectors.postgresql import PostgreSqlClient
from assets.air_quality import load
from datetime import datetime

@pytest.fixture
def setup_extract():
    load_dotenv()
    return os.environ.get("API_KEY")

@pytest.fixture
def setup_input_df_population():
    return extract_cities_data(
        "project_tests/data/city_data.csv"
    )

def extract_cities_data(setup_extract):
    API_KEY = setup_extract
    weather_api_client = WeatherApiClient(api_key=API_KEY)
    df = extract_weather(
        weather_api_client=weather_api_client,
        city_reference_path=Path(
            "./etl_project_tests/data/weather/australian_capital_cities.csv"
        ),
    )
    assert len(df) == 8