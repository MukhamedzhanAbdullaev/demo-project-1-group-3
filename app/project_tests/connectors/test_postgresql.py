from  project.connectors.postgresql import PostgreSqlClient
import pytest
from dotenv import load_dotenv
import os
from sqlalchemy import Table, Column, Integer, String, MetaData, Float


@pytest.fixture
def setup_postgresql_client():
    load_dotenv()
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("PORT")

    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )
    return postgresql_client


@pytest.fixture
def setup_table():
    table_name = "test_table"
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("iso_datetime", String),
        Column("city", String),
    )
    return table_name, table, metadata


def test_postgresqlclient_insert(setup_postgresql_client, setup_table):
    postgresql_client = setup_postgresql_client
    table_name, table, metadata = setup_table
    postgresql_client.drop_table(table_name)  # make sure table has already been dropped

    data = [   
                {
                    "id": 1, 
                    "iso_datetime": "2024-01-17T04:00:00+08:00",
                    "city": "Mumbai"
                }, 
                {
                    "id": 2, 
                    "iso_datetime": "2024-01-17T01:00:00+06:00",
                    "city": "Osaka"
                },
                {
                    "id": 3, 
                    "iso_datetime": "2024-01-16T21:00:00+02:00",
                    "city": "Cairo"
                }
            ]

    postgresql_client.insert(data=data, table=table, metadata=metadata)

    result = postgresql_client.select_all(table=table)
    assert len(result) == 3

    postgresql_client.drop_table(table_name)
