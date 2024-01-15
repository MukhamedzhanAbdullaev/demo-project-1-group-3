from dotenv import load_dotenv
import os
from connectors.air_quality_api import AirQualityApiClient
from sqlalchemy import Table, MetaData, Column, Integer, String, Float
from assets.air_quality import (
    extract_air_quality
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

    df_aq, df_cities = extract_air_quality(
        air_quality_api_client=air_quality_api_client,
        city_reference_path=config.get("city_reference_path"),
    )

#transform-----
print('hello')