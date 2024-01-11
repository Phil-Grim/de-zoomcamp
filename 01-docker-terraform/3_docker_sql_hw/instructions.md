Build the docker image from this folder:

```bash
docker build -t taxi_ingest:v002 .
```

Run the docker image - which should run the pipeline from hw_ingest_data.py, uploading the csv data into postgres.
    *  Having used docker-compose to create pgdatabase and pgadmin, will need to find the network that was created in that process - by using docker network ls (was 2_docker_sql_default in my case).

```bash
docker run -it \
    --network=2_docker_sql_default \
    taxi_ingest:v002 \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"
    ```