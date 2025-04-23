import os
import requests

import duckdb
from dagster_essentials.assets import constants
from dagster._utils.backoff import backoff
from dagster_duckdb import DuckDBResource

from dagster import asset

@asset
def taxi_zones_file() -> None:
    """
        the raw csv file for the taxi zones dataset
    """
    raw_zones = requests.get(
        f"https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
    )

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_zones.content)

@asset(
        deps=["taxi_zones_file"]
)
def taxi_zones(database: DuckDBResource) -> None:
    """
    the raw taxi zones dataset loaded into a DuckDB database
    """
    query = f"""
        create or replace table zones as (
            select
                LocationID as zone_id,
                zone,
                borough,
                the_geom as geometry
            from '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """


    with database.get_connection() as conn:
        conn.execute(query)

