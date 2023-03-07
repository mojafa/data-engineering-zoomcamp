from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def ingest_data(dataset_url):
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def transform_data(df):
    # """Fix dtype issues and add new columns"""
    # df['tpep_pickup_datetime']  = pd.to_datetime(df['tpep_pickup_datetime'])
    # df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    # print(df.head(2))
    # print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def export_data_local(df, color, dataset_file):
    path = Path(f"./data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task(log_prints=True)
def export_data_gcs(path):
    gcs_block = GcsBucket.load("dezoomcamp")
    gcs_block.upload_from_path(from_path=path, to_path=path)

    return path


@flow()
def etl_web_to_gcs(year, month, color):
    # """Main ETL function"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url  = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = ingest_data(dataset_url)
    df_clean = transform_data(df)
    path = export_data_local(df_clean, color, dataset_file)
    export_data_gcs(path)

@flow()
def etl_parent_flow(color: str = "yellow", months: list[int] = [1,2], year: int = 2021):
    for month in months:
        etl_web_to_gcs(year, month, color)

if __name__ == "__main__":
    color="yellow"
    months=[2,3]
    year=2019
    etl_parent_flow(color, months, year)
