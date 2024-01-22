from dotenv import load_dotenv
import os
from project.connectors.air_quality_api import AirQualityApiClient
from project.connectors.postgresql import PostgreSqlClient
from project.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
from project.assets.pipeline_logging import PipelineLogging
from sqlalchemy import Table, MetaData, Column, Integer, String, Float
from project.assets.air_quality import (
    extract_cities_data,
    extract_air_quality,
    transform,
    load
)
import yaml
from pathlib import Path
import schedule
import time

def pipeline(config: dict, pipeline_logging: PipelineLogging):
    pipeline_logging.logger.info("Starting pipeline run")
    # set up environment variables
    pipeline_logging.logger.info("Getting pipeline environment variables")
    API_KEY = os.environ.get("API_KEY")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("PORT")

    pipeline_logging.logger.info("Creating Air Quality API client")
    air_quality_api_client = AirQualityApiClient(api_key=API_KEY, pipeline_logging=pipeline_logging)
    # extract
    pipeline_logging.logger.info("Extracting data from Air Quality API and CSV file")
    df_cities = extract_cities_data(
        city_reference_path=config.get("city_reference_path"),
    )
    df_aq = extract_air_quality(
        air_quality_api_client=air_quality_api_client,
        df_cities=df_cities,
    )
    # transform
    pipeline_logging.logger.info("Transforming dataframes with pandas")
    df_transformed = transform(df_aq=df_aq, df_cities=df_cities)
    print(df_transformed.head())
    # load
    pipeline_logging.logger.info("Loading data to postgres")
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
        Column("population/km2", Integer),
        Column("aqi", Integer),
        Column("aqi_rank", Integer),
        Column("air_pollution_level", String),
        Column("temperature", Float),
        Column("humidity", Float),
        Column("pm10", Float),
        Column("pm2.5", Float),
    )
    load(
        df=df_transformed,
        postgresql_client=postgresql_client,
        table=table,
        metadata=metadata,
        load_method="upsert",
    )
    pipeline_logging.logger.info("Pipeline run successful")


def run_pipeline_schedule(
    pipeline_name: str,
    postgresql_logging_client: PostgreSqlClient,
    pipeline_config: dict,
):
    pipeline_logging = PipelineLogging (
        pipeline_name=pipeline_name,
        log_folder_path=pipeline_config.get("config").get("log_folder_path"),
    )
    metadata_logger = MetaDataLogging(
        pipeline_name=pipeline_name,
        postgresql_client=postgresql_logging_client,
        config=pipeline_config.get("config"),
    )
    try:
        metadata_logger.log()  # log start
        pipeline(
            config=pipeline_config.get("config"), pipeline_logging=pipeline_logging
        )
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs()
        )  # log end
        pipeline_logging.logger.handlers.clear()
    except BaseException as e:
        pipeline_logging.logger.error(f"Pipeline run failed. See detailed logs: {e}")
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logging.get_logs()
        )  # log error
        pipeline_logging.logger.handlers.clear()

if __name__ == "__main__":
    load_dotenv()
    LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER_NAME")
    LOGGING_DATABASE_NAME = os.environ.get("LOGGING_DATABASE_NAME")
    LOGGING_USERNAME = os.environ.get("LOGGING_USERNAME")
    LOGGING_PASSWORD = os.environ.get("LOGGING_PASSWORD")
    LOGGING_PORT = os.environ.get("LOGGING_PORT")

    postgresql_logging_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_USERNAME,
        password=LOGGING_PASSWORD,
        port=LOGGING_PORT
    )

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
    
    #set schedue
    schedule.every(pipeline_config.get("schedule").get("run_seconds")).seconds.do(
        run_pipeline_schedule,
        pipeline_name=PIPELINE_NAME,
        postgresql_logging_client=postgresql_logging_client,
        pipeline_config=pipeline_config
    )

    while True:
        schedule.run_pending()
        time.sleep(pipeline_config.get("schedule").get("poll_seconds"))
