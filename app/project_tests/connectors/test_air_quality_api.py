from dotenv import load_dotenv
from project.connectors.air_quality_api import AirQualityApiClient
from project.assets.pipeline_logging import PipelineLogging
import os
import pytest


@pytest.fixture
def setup():
    load_dotenv()


def test_air_quality_client_get_city_by_name(setup):
    API_KEY = os.environ.get("API_KEY")
    pipeline_logging = PipelineLogging (
        pipeline_name="air_quality_etl",
        log_folder_path="./project_tests/logs"
    )
    air_quality_api_client = AirQualityApiClient(api_key=API_KEY, pipeline_logging=pipeline_logging)
    data = air_quality_api_client.get_air_quality(city="Qingdao")
    assert type(data) == dict
    assert len(data) > 10