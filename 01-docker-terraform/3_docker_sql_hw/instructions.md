Build the docker image from this folder:

```bash
docker build -t taxi_ingest:v002 .
```

Run the docker image - which should run the pipeline from hw_ingest_data.py, uploading the csv data into postgres

```bash
docker run -it \
    --network=pg-network \
    taxi_ingest:v002 \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"
    ```