#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from time import time
from datetime import timedelta
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash # allows us to cache results of a task (to speed up computation)


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    df_iter = pd.read_csv(csv_name, sep=',', iterator=True, chunksize=100000, on_bad_lines='warn')

    df = next(df_iter) # df will just be the first 100000 rows of csv_name 

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df

@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}") # gives us the no. of rows where passenger count = 0
    df = df[df['passenger_count'] != 0] # selecting only the rows where passenger count > 0
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df

@task(log_prints=True, retries=3)
def load_data(user, password, host, port, db, table_name, df):
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


@flow(name='Subflow', log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging Subflow for: {table_name}") # just demonstrating subflows; no purpose in this pipeline

@flow(name='Ingest flow')
def main_flow(table_name: str):
    # could have the below as arguments in this function (e.g. use the argparse library - see ingest_data.py from wk 1.02)
    # added table_name as an argument
    user='root'
    password='root'
    host='localhost'
    port='5432'
    db='ny_taxi'
    table_name=table_name
    csv_url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    log_subflow(table_name)
    raw_data = extract_data(csv_url) 
    data = transform_data(raw_data)
    load_data(user, password, host, port, db, table_name, data)

if __name__ == '__main__':
    
    main_flow('wk_2_yellow_taxi_trips')
