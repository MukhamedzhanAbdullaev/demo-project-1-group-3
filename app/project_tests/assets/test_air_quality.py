import os
from pathlib import Path
from project.assets.air_quality import extract_cities_data, extract_air_quality, transform
from project.connectors.air_quality_api import AirQualityApiClient
from project.assets.pipeline_logging import PipelineLogging
import pytest
from dotenv import load_dotenv
import pandas as pd

@pytest.fixture
def setup_extract():
    load_dotenv()
    return os.environ.get("API_KEY")

def test_extract_air_quality_data(setup_extract):
    API_KEY = setup_extract
    pipeline_logging = PipelineLogging (
        pipeline_name="air_quality_etl",
        log_folder_path="./project_tests/logs"
    )
    air_quality_api_client = AirQualityApiClient(api_key=API_KEY, pipeline_logging=pipeline_logging)
    df_cities = extract_cities_data(
        "./project_tests/data/city_data.csv"
    )
    df_aq = extract_air_quality(
        air_quality_api_client=air_quality_api_client,
        df_cities=df_cities,
    )
    assert len(df_cities) == 81
    assert len(df_aq) == 81

@pytest.fixture
def setup_input_df_aq():
    return pd.DataFrame(
        [
            {
                "city_name": "tokyo",
                "aqi": 20.0,
                "co.v": 4.5,
                "h.v": 62.0,
                "no2.v": 32.5,
                "o3.v": 6.4,
                "p.v": 1022.4,
                "pm10.v": 20.0,
                "pm25.v": 1.0,
                "r.v": 1.8,
                "so2.v": 6.8,
                "t.v": 8.8,
                "w.v": 1.5,
                "wd.v":22.5,
                's': '2024-01-13 05:00:00',
                'tz': '+09:00',
                "v": "1.705640e+09",
                "iso": "2024-01-19T05:00:00+09:00",
            },
            {
                "city_name": "mexico-city",
                "aqi": 59.0,
                "co.v": 12.3,
                "h.v": 48.5,
                "no2.v": 39.9,
                "03.v": 4.0,
                "p.v": 1030.1,
                "pm10.v": 37.0,
                "pm25.v": 59.0,
                "r.v": 1.8,
                "so2.v": 6.8,
                "t.v": 19.0,
                "w.v": 1.5,
                "wd.v":22.5,
                's': '2024-01-13 10:00:00',
                'tz': '+09:00',
                "v": "1.695809e+09",
                "iso": "2023-09-27T10:00:00-05:00",
            },
        ]
    )

@pytest.fixture
def setup_input_df_cities():
    return extract_cities_data(
        "./project_tests/data/city_data.csv"
    )

@pytest.fixture
def setup_transformed_df():
    return pd.DataFrame(
        [
            {
                "city": "Tokyo",
                "country": "Japan",
                "population": 13515271,
                "aqi": 20,
                "humidity": 62.0,
                "temperature": 8.8,
                "pm10": 20.0,
                "pm2.5": 1.0,
                "iso_datetime": "2024-01-19T05:00:00+09:00",
                "aqi_rank": 1,
                "population/km2": 6168,
                "air_pollution_level": "Good"
            },
            {
                "city": "Mexico City",
                "country": "Mexico",
                "population": 9209944,
                "aqi": 59,
                "humidity": 48.5,
                "temperature": 19.0,
                "pm10": 37.0,
                "pm2.5": 59.0,
                "iso_datetime": "2023-09-27T10:00:00-05:00",
                "aqi_rank": 2,
                "population/km2": 6201,
                "air_pollution_level": "Moderate"
            },
        ]
    )

def test_transform(
    setup_input_df_cities, setup_input_df_aq, setup_transformed_df
):
    df_cities = setup_input_df_cities
    df_aq = setup_input_df_aq
    expected_df = setup_transformed_df
    df = transform(df_aq=df_aq, df_cities=df_cities)
    pd.testing.assert_frame_equal(left=df, right=expected_df, check_exact=True)
