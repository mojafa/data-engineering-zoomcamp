from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color, year, month) -> Path:
    """Data Extract from Google Cloud Storage"""
    gcs_path = f"../../data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("dezoomcamp")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"data/")
    print(gcs_path)
    return Path(f"{gcs_path}")

@task()
def transform(path) -> pd.DataFrame:
    """Data Cleaning"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df) -> None:
    """Write DataFrame to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("dezoomcamp-gcp-creds")

    df.to_gbq(
        destination_table="trips_data_all.rides",
        project_id="smart-grin-376216",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq():
    color = "yellow"
    year  = 2020
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()
