from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_gcp import GcpCredentials


##########################################################
######## etl_web_to_gcs ########
##########################################################
@task(retries=3)
def ingest_data(dataset_url):
    df = pd.read_csv(dataset_url)

    return df

@task(log_prints=True)
def transform_data(df):
    df['tpep_pickup_datetime']  = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    return df

@task(log_prints=True)
def export_data_local(df, color, dataset_file):
    path = Path(f"../../data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task(log_prints=True)
def export_data_gcs(path):
        gcs_block = GcsBucket.load("dezoomcamp")
        gcs_block.upload_from_path(from_path=path, to_path=path)



@flow()
def etl_web_to_gcs(year, month, color):
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = ingest_data(dataset_url)
    df_clean = transform_data(df)
    path = export_data_local(df_clean, color, dataset_file)
    export_data_gcs(path)


##########################################################
######## etl_gcs_to_bq ########
##########################################################

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
def etl_gcs_to_bq(year, month, color):
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


##########################################################
######## parent flow ########
##########################################################
@flow()
def etl_parent_flow(months: list[int] = [1,2], year: int = 2021, color: str = "yellow"):
    for month in months:
        etl_web_to_gcs(year, month, color)
        etl_gcs_to_bq(year, month, color)


if __name__ == "__main__":
    color="yellow"
    months=[1,2,3]
    year=2021
    etl_parent_flow(months, year, color)
