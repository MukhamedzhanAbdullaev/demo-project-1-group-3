import os
from pathlib import Path
import pytest
from dotenv import load_dotenv
import pandas as pd
from assets.air_quality import (
    extract_cities_data,
    transform,
)



@pytest.fixture
def setup_input_df_air_quality():
    return pd.DataFrame(
        [
            {
                'city_name': 'qingdao',
                'aqi': 132.0,
                'co.v': 0.1,
                'd.v': -12,
                'h.v': 86,
                'no2.v': 14.7,
                'o3.v': 13,
                'p.v': 1030,
                'pm10.v': 59,
                'pm25.v': 132,
                'so2.v': 3.6,
                't.v': -2,
                'w.v': 2.5,
                'wd.v': 190,
                's': '2024-01-19 02:00:00',
                'tz': '+08:00',
                'v': 1705629600,
                'iso': '2024-01-19T02:00:00+08:00',
            }
        ]
    )


@pytest.fixture
def setup_input_df_cities():
    return extract_cities_data(
        "./data/city_data.csv"
    )

@pytest.fixture
def setup_transformed_df():
    return pd.DataFrame(
        [
            {
                'city': {0: 'Qingdao'},
                'country': {0: 'China'},
                'population': {0: 10071722},
                'aqi': {0: 132},
                'humidity': {0: 86},
                'temperature': {0: -2},
                'pm10': {0: 59},
                'pm2.5': {0: 132},
                'iso_datetime': {0: '2024-01-19T02:00:00+08:00'},
                'aqi_rank': {0: 1},
                'population/km2': {0: 896},
                'air_pollution_level': {0: 'Unhealthy if Sensitive'}
            }
        ]
    )



def test_transform(
    setup_input_df_air_quality, setup_input_df_cities, setup_transformed_df
):
    df_aq = setup_input_df_air_quality
    df_cities = setup_input_df_cities
    expected_df = setup_transformed_df
    df = transform(df_aq=df_aq, df_cities=df_cities)
    print(df.info())
    print(expected_df.info())
    pd.testing.assert_frame_equal(left=df, right=expected_df, check_exact=True)