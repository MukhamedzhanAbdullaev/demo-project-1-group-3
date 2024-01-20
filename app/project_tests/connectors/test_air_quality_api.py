from dotenv import load_dotenv
from project.connectors.air_quality_api import AirQualityApiClient
import os
import pytest


@pytest.fixture
def setup():
    load_dotenv()


def test_air_quality_client_get_city_by_name(setup):
    API_KEY = os.environ.get("API_KEY")
    air_quality_api_client = AirQualityApiClient(api_key=API_KEY)
    data = air_quality_api_client.get_air_quality(city="Qingdao")
    print(data)
    assert type(data) == dict
    assert len(data) > 10