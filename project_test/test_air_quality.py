import pandas as pd
from dotenv import load_dotenv
from connectors.air_quality_api import AirQualityApiClient
import os
import pytest
import numpy as np

@pytest.fixture
def setup():
    load_dotenv()

def test_air_quality(setup):
    API_KEY = os.environ.get("API_KEY")

    air_quality_api_client = AirQualityApiClient(api_key=API_KEY)

    # df_cities = pd.read_csv("")
    # aq_data = []
    # for city in df_cities["city"]:
    #     city_parsed = city.replace(" ", "-").lower()
    #     aq_data.append(air_quality_api_client.get_air_quality(city_name=city_parsed))
    data = air_quality_api_client.get_air_quality(city_name="Tokyo")
    assert type(data) == dict
    assert pd.Series(data["city_name"]).is_unique
    assert (type(data["city_name"]) == str)