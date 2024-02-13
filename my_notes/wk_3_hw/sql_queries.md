
## Set Up 

[link to the homework](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/03-data-warehouse/homework.md)

Run the `hw_web_to_gcs.py` script

Create the materialised table from the external table 'trips_data_all.2022_green' that was created using the above script:

```sql
CREATE TABLE trips_data_all.2022_green_table AS
SELECT * 
FROM trips_data_all.2022_green
```

## Q1


Count of rows present in the metadata (Details tab) of the materialised table, but not the external table. Can run a count(*) query to get the number of rows

```sql
SELECT count(*)
FROM trips_data_all.2022_green
```

## Q2

```sql
SELECT COUNT(DISTINCT(PULocationID))
FROM trips_data_all.2022_green
```

Run the same query for the materialised view and compare the estimated data that will be read for each query

## Q3


```sql
SELECT COUNT(*)
FROM trips_data_all.2022_green
WHERE fare_amount = 0
```

## Q4

Creating the clustered and partitioned table:

```sql
CREATE TABLE trips_data_all.clustered_partitioned
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS 
SELECT * FROM trips_data_all.2022_green_table;
```


## Q5

running the query on the non-partitioned and partitioned tables to see how many bytes are processed for each

```sql
SELECT DISTINCT(PULocationID), lpep_pickup_datetime
FROM trips_data_all.2022_green_table
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-01-06' and '2022-06-30';

SELECT DISTINCT(PULocationID)
FROM trips_data_all.clustered_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-01-06' and '2022-06-30'
```

## Q8

0 bytes processed - presumably because count(*) is stored in the metadata (as total rows)

```sql
SELECT count(*)
FROM trips_data_all.2022_green_table
```