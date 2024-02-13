import io
import os
import requests
import pandas as pd
from prefect import task, flow
from google.cloud import storage
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage prefect prefect_gcp[bigquery]`
2. Set up a Prefect Cloud account, create an API key and log in via the terminal using the API key
3. Created GCS bucket called 'homework-bucket-evident-display-410312'
4. Created GCS block in Prefect cloud for the above bucket (using the GCP credentials block I already had) - called de-zoomcamp
5. trips_data_all BigQuery dataset already exists in my project
"""


@task(name='download files')
def web_to_gcs():
    for i in range(12):

        month = '0' + str(i+1)
        month = month[-2:]

        file_name = f"green_tripdata_2022-{month}.parquet"
        file_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"

        r = requests.get(file_url)
        file_path = f"data/{file_name}"
        open(file_path, 'wb').write(r.content)

        gcs_block = GcsBucket.load("de-zoomcamp")
        gcs_block.upload_from_path(
            from_path=file_path,
            to_path=file_name
        )
        


@task(name='create external table')
def create_external_table():
    """Creates an external table, if it doesn't already exist, pointing to the bucket storage location
    where the daily raw london-properties parquet files are stored"""

    gcp_credentials = GcpCredentials.load('london-properties-analysis')

    with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
        warehouse.execute(
            """CREATE EXTERNAL TABLE IF NOT EXISTS trips_data_all.2022_green
            OPTIONS (format = 'PARQUET', uris=['gs://homework-bucket-evident-display-410312/*.parquet'])"""
        )


@flow
def main():
    # web_to_gcs()
    create_external_table()


if __name__ == '__main__':
    main()
