from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os
from prefect.blocks.notifications import SlackWebhook


@task(retries=3)
def ingest_data(dataset_url):
    df = pd.read_csv(dataset_url)

    print(df.head(2))
    print(f"colums: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def transform_data(df):
    # df['tpep_pickup_datetime']  = pd.to_datetime(df['tpep_pickup_datetime'])
    # df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    return df

@task()
def export_data_local(df, color, dataset_file):
    newpath = 'data/green/'
    if not os.path.exists(newpath):
        os.makedirs(newpath)

    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    print(df.shape[0])
    return path


@task(log_prints=True)
def export_data_gcs(path):
    gcs_block = GcsBucket.load("dezoomcamp")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    slack_webhook_block = SlackWebhook.load("zoomcamp-slack")
    slack_webhook_block.notify("Hello from Prefect!")
    return path


@flow()
def etl_web_to_gcs():
    color = "green"
    year  = 2019
    month = 4
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url  = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = ingest_data(dataset_url)
    df_clean = transform_data(df)
    path = export_data_local(df_clean, color, dataset_file)
    export_data_gcs(path)

if __name__ == "__main__":
    etl_web_to_gcs()
