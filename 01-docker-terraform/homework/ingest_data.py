#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from tqdm.auto import tqdm
from pathlib import Path


# -----------------------
# Configuration
# -----------------------

TRIP_DATA_PATH = Path("data/green_tripdata_2025-11.parquet")
ZONE_DATA_PATH = Path("data/taxi_zone_lookup.csv")

DTYPE_MAP = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

PARSE_DATES = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
]


# -----------------------
# Loaders
# -----------------------

def load_trip_data(engine, target_table, chunksize, parquet_path=TRIP_DATA_PATH):
    """Load green taxi trip data from Parquet into Postgres."""

    parquet_file = pq.ParquetFile(parquet_path)
    first_batch = True

    for batch in tqdm(parquet_file.iter_batches(batch_size=chunksize), desc="Loading trip data"):
        df = batch.to_pandas()

        # Apply dtype only to existing columns
        dtype = {k: v for k, v in DTYPE_MAP.items() if k in df.columns}
        df = df.astype(dtype, errors="ignore")

        # Parse datetimes safely
        for col in PARSE_DATES:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        if first_batch:
            df.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace",
                index=False,
            )
            first_batch = False

        df.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10_000,
        )


def load_zone_data(engine, target_table, csv_path=ZONE_DATA_PATH):
    """Load taxi zone lookup data."""

    df = pd.read_csv(csv_path)

    df.to_sql(
        name=target_table,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
    )


# -----------------------
# CLI
# -----------------------

@click.command()
@click.option("--pg-user", default="postgres", show_default=True)
@click.option("--pg-pass", default="postgres", show_default=True)
@click.option("--pg-host", default="localhost", show_default=True)
@click.option("--pg-port", default=5433, type=int, show_default=True)
@click.option("--pg-db", default="ny_taxi", show_default=True)
@click.option("--trip-table", default="green_trip_data", show_default=True)
@click.option("--zone-table", default="zones", show_default=True)
@click.option("--chunksize", default=100_000, type=int, show_default=True)
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, trip_table, zone_table, chunksize):
    """Ingest NYC Taxi data into PostgreSQL."""

    engine = create_engine(
        f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    load_trip_data(engine, trip_table, chunksize)
    load_zone_data(engine, zone_table)


if __name__ == "__main__":
    run()

