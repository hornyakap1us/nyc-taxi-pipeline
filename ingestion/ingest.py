"""
ingest.py
---------
Copies a monthly partition of NYC yellow taxi trips from the
BigQuery public dataset into your own raw dataset.

Usage:
    python ingest.py                        # defaults to 2022, month 1
    python ingest.py --year 2022 --month 3  # specific month
"""

import argparse
import os

from dotenv import load_dotenv
from google.cloud import bigquery

from bq_client import get_client

load_dotenv()

SOURCE_TABLE = "bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022"
RAW_DATASET  = os.environ.get("BQ_RAW_DATASET", "raw")
RAW_TABLE    = "yellow_trips"


def ingest_month(year: int, month: int, client: bigquery.Client) -> int:
    """
    Copy one month of trips from the public dataset
    into raw.yellow_trips (append).
    Returns the number of rows written.
    """
    project    = os.environ["GCP_PROJECT_ID"]
    dest_table = f"{project}.{RAW_DATASET}.{RAW_TABLE}"

    sql = f"""
        SELECT
            vendor_id,
            pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_distance,
            pickup_location_id,
            dropoff_location_id,
            rate_code,
            store_and_fwd_flag,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            imp_surcharge,
            airport_fee,
            total_amount
        FROM `{SOURCE_TABLE}`
        WHERE data_file_year = {year}
          AND data_file_month = {month}
    """

    job_config = bigquery.QueryJobConfig(
        destination=dest_table,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="pickup_datetime",
        ),
    )

    job = client.query(sql, job_config=job_config)
    job.result()

    rows_written = job.num_dml_affected_rows or 0
    print(f"  [ingest] {year}-{month:02d} → {dest_table} ({rows_written:,} rows)")
    return rows_written


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--year",  type=int, default=2022)
    p.add_argument("--month", type=int, default=1)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    client = get_client()
    ingest_month(args.year, args.month, client)