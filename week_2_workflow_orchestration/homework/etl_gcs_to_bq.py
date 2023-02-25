from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("dezoomcamp")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./")
    return Path(f"{gcs_path}")

@task()
def read(path: Path) -> pd.DataFrame:
    """read the data into pandas"""
    df = pd.read_parquet(path)
    return df


@task()
def write_bq(df: pd.DataFrame) -> int:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("dezoomcamp-gcp-creds")

    df.to_gbq(
        destination_table="trips_data_all.rides",
        project_id="smart-grin-376216",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )
    return len(df)


@flow()
def el_gcs_to_bq(year: int, month: int, color: str) -> None:
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs(color, year, month)
    df = read(path)
    row_count = write_bq(df)
    return row_count


@flow(log_prints=True)
def el_parent_gcs_to_bq(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    """Main EL flow to load data into Big Query"""
    total_rows = 0

    for month in months:
        rows = el_gcs_to_bq(year, month, color)
        total_rows += rows

    print(total_rows)


if __name__ == "__main__":
    el_parent_gcs_to_bq(months=[2, 3], year=2019, color="yellow")
