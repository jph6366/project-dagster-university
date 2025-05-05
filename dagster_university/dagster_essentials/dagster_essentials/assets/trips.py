import os
import requests
import duckdb
import pandas as pd
from dagster_essentials.assets import constants
from dagster._utils.backoff import backoff
from dagster_duckdb import DuckDBResource
from dagster_essentials.partitions import monthly_partition

from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue

@asset(
    partitions_def=monthly_partition,
    group_name="raw_files",
)
def taxi_trips_file(context: AssetExecutionContext) -> MaterializeResult:
    """
        the raw parquet files for the taxi trips dataset
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
    
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)

    num_rows = len(pd.read_parquet(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)))

    return MaterializeResult(
        metadata={
            'Number of records': MetadataValue.int(num_rows)
        }
    )

@asset(
    deps=["taxi_trips_file"],
    partitions_def=monthly_partition,
    group_name="ingested",
)
def taxi_trips(context: AssetExecutionContext, database: DuckDBResource) -> None:
    """
      The raw taxi trips dataset, loaded into a DuckDB database
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]

    query = f"""
      create table if not exists trips (
        vendor_id integer, pickup_zone_id integer, dropoff_zone_id integer,
        rate_code_id double, payment_type integer, dropoff_datetime timestamp,
        pickup_datetime timestamp, trip_distance double, passenger_count double,
        total_amount double, partition_date varchar
      );

      delete from trips where partition_date = '{month_to_fetch}';

      insert into trips
      select
        VendorID, PULocationID, DOLocationID, RatecodeID, payment_type, tpep_dropoff_datetime,
        tpep_pickup_datetime, trip_distance, passenger_count, total_amount, '{month_to_fetch}' as partition_date
      from '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';
    """

    with database.get_connection() as conn:
        conn.execute(query)
