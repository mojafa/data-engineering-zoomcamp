#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from time import time
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector



@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df


@task(log_prints=True, tags=["extract"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    parquet_file = 'output.parquet'
    csv_file = 'output.csv'
    os.system(f'wget {url} -O {parquet_file}')
    df = pd.read_parquet(parquet_file, engine = 'pyarrow')
    df.to_csv(csv_file, index=False)

    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)
    df = next(df_iter)
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    return df



@task(log_prints=True, retries=3)
def ingest_data( table_name, df):
    # we'll use sql alchemy connector instead of hardcoding the connection string
    # postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    # engine = create_engine(postgres_url)
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')


@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging Subflow for: {table_name}")

@flow(name='Ingest flow')
def main_flow(table_name: str = 'green_taxi_trips'):
    # table_name = 'green_taxi_trips'
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet'

    raw_data = extract_data(url)
    data = transform_data(raw_data)
    ingest_data(table_name, data)


if __name__ == '__main__':
    main_flow()
