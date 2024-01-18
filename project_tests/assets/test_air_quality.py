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