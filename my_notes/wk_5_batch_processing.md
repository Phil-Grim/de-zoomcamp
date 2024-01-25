
### Contents

- [Introduction to Spark](#introduction-to-spark)
    - [What we're covering this week](#what-were-covering-this-week)
    - [Batch v Streaming](#batch-vs-streaming)
    - [Types of Batch Jobs](#types-of-batch-jobs)
    - [Orchestrating Batch Jobs](#orchestrating-batch-jobs)
    - [Pros and Cons of Batch Jobs](#pros-and-cons-of-batch-jobs)
    - [What is Spark](#what-is-spark)
    - [Why do we need Spark?](#why-do-we-need-spark)
- [Installing Spark](#installing-spark)
- [First Look at Spark/PySpark](#first-look-at-sparkpyspark)
    - [Initial Set Up](#initial-set-up)
    - [Creating a Spark Session](#creating-a-spark-session)
    - [Reading CSV files](#reading-csv-files)
    - [Partitions](#partitions)
    - [Spark Dataframes](#spark-dataframes)
    - [Actions and Transformations](#actions-and-transformations)
    - [Functions and UDFs](#functions-and-user-defined-functions-udfs)
- [Preparing Yellow and Green Taxi Data](#preparing-green-and-yellow-taxi-data)
    - [Download the Datasets](#download-the-datasets)
    - [Parquetise the Datasets](#parquetise-the-datasets)
- [SQL with Spark](#sql-with-spark)
    - [Combining the 2 datasets](#combining-two-datasets)
    - [Querying a Dataset with Temporary Tables](#querying-a-dataset-with-temporary-tables)
- [Spark Internals](#spark-internals)
    - [Spark Clusters](#spark-clusters)
    - [GroupBy in Spark](#groupby-in-spark)
    - [Joins in Spark](#joins-in-spark)
        - [Joining 2 Large Tables](#joining-2-large-tables)
        - [Joining 1 large and 1 small table](#joining-a-large-table-and-a-small-table)
- [Resilient Distributed Datasets](#resilient-distributed-datasets)
    - [RDDS: Map and Reduce](#rdds-map--reduce)
        - [Operations on RDDs: map, filter, reduceByKey](#operations-on-rdds-map-filter-reducebykey)
        - [From RDD to DataFrame](#from-rdd-to-dataframe)
    - [Spark RDD mapPartitions](#spark-rdd-mappartitions)
        - [Using mapPartition for ML](#using-mappartitions-for-ml)
- [Running Spark in the Cloud](#running-spark-in-the-cloud)
    - [Connecting to Google Cloud Storage](#connecting-to-google-cloud-storage)
        - [Uploading files to Cloud Storage with gsutil](#uploading-files-to-cloud-storage-with-gsutil)
        - [Configuring Spark with the GCS connector](#configuring-spark-with-the-gcs-connector)
        - [Reading the remote data](#reading-the-remote-data)
    - [Creating a local spark cluster](#creating-a-local-spark-cluster)
        - [Spark standalone master and workers](#spark-standalone-master-and-workers)
        - [Parameterizing our scripts for Spark](#parametrizing-our-scripts-for-spark)
        - [Submitting spark jobs with spark submit](#submitting-spark-jobs-with-spark-submit)
    - [Setting up a Dataproc Cluster](#setting-up-a-dataproc-cluster)
        - [Creating the cluster](#creating-the-cluster)
        - [Running a job with the web ui](#running-a-job-with-the-web-ui)
        - [Running a job with the gcloud SDK](#running-a-job-with-the-gcloud-sdk)
        - [Connecting Spark to BigQuery](#connecting-spark-to-bigquery)


# Introduction to Spark

## What we're covering this week:

- Spark, Spark DataFrames, and Spark SQL
- Joins in Spark
- Resilient Distributed Datasets (RDDs)
- Spark internals
- Spark with Docker
- Running Spark in the Cloud
- Connecting Spark to a Data Warehouse (DWH)

## Batch vs Streaming

There are 2 ways of processing data:
* ***Batch processing***: processing _chunks_ of data at _regular intervals_.
    * Example: processing taxi trips each month.
        ```mermaid
        graph LR;
            a[(taxi trips DB)]-->b(batch job)
            b-->a
        ```
* ***Streaming***: processing data _on the fly_.
    * Example: processing a taxi trip as soon as it's generated.
        ```mermaid
        graph LR;
            a{{User}}-. gets on taxi .->b{{taxi}}
            b-- ride start event -->c([data stream])
            c-->d(Processor)
            d-->e([data stream])
        ```

This week will cover ***batch processing***. Next week will cover streaming.

## Types of Batch Jobs


A batch job is a job that will process data in batches. They can be scheduled e.g. weekly, daily (common), hourly (common), x times per hour, etc.

Batch jobs may also be created using:

* Python scripts
    * Python scripts can be run anywhere (Kubernets, AWS Batch, ...)
* SQL - as you'd do in DBT
* Spark (what we will use for this lesson)
* Flink
* Etc...

## Orchestrating Batch Jobs

A common workflow might involve:

```mermaid
  graph LR;
      A(DataLake CSV)-->B(Python);
      B-->C[(SQL-dbt)]
      C-->D(Spark)
      D-->E(Python)
```

Each step would involve batch processing - and would be orchestrated via e.g. Mage, Prefect, Airflow

## Pros and cons of batch jobs

* Advantages:
    * Easy to manage. There are multiple tools to manage them (the technologies we already mentioned)
    * Re-executable. Jobs can be easily retried if they fail.
    * Scalable. Scripts can be executed in more capable machines; Spark can be run in bigger clusters, etc.
* Disadvantages:
    * Delay. Each task of the workflow in the previous section may take a few minutes; assuming the whole workflow takes 20 minutes, we would need to wait those 20 minutes until the data is ready for work.

Most companies that deal with data tend to work with batch jobs most of the time (probably 90%) - it's good enough for most things.

## What is Spark?

[Apache Spark](https://spark.apache.org/) is an open-source ***multi-language*** unified analytics ***engine*** for large-scale data processing.

Spark is an ***engine*** because it _processes data_.

```mermaid
graph LR;
    A[(Data Lake)]-->|Pulls data|B(Spark)
    B-->|Does something to data|B
    B-->|Outputs data|A
```

Spark can be ran in _clusters_ with multiple _nodes_, each pulling and transforming data.

Spark is ***multi-language*** because we can use Java and Scala natively, and there are wrappers for Python, R and other languages.

The wrapper for Python is called [PySpark](https://spark.apache.org/docs/latest/api/python/).

Spark can deal with both batches and streaming data. Streaming data can be seen as a sequence of small batches, and so you can apply similar techniques as those that are used for regulat batches. 

## Why do we need Spark?

Spark is used for transforming data in a Data Lake.

There are tools such as Hive, Presto or Athena (an AWS managed Presto) that allow you to express jobs as SQL queries. However, there are times where you need to apply more complex manipulation which are very difficult or even impossible to express with SQL (such as ML models); in those instances, Spark is the tool to use.

```mermaid
graph LR;
    A[(Data Lake)]-->B{Can the <br /> job be expressed <br /> with SQL?}
    B-->|Yes|C(Hive/Presto/Athena)
    B-->|No|D(Spark)
    C & D -->E[(Data Lake)]
```

A typical workflow may combine both tools. Here's an example of a workflow involving Machine Learning:

```mermaid
graph LR;
    A((Raw data))-->B[(Data Lake)]
    B-->C(SQL Athena job)
    C-->D(Spark job)
    D-->|Train a model|E(Python job <br /> Train ML)
    D-->|Use a model|F(Spark job <br /> Apply model)
    E-->G([Model])
    G-->F
    F-->|Save output|B
```

In this scenario, most of the preprocessing would be happening in Athena, so for everything that can be expressed with SQL, it's always a good idea to do so, but for everything else, there's Spark.

# Installing Spark

Installed Java and Spark on my GCP VM - using the instructions from [linux.md](../05-batch/setup/linux.md).

Instructions for MacOS and Windows are also available in the setup folder of week 5.

After installing the appropriate JDK (Java) and Spark, set up PySpark using the following [instructions](../05-batch/setup/pyspark.md)

# First Look at Spark/Pyspark 

## Initial Set Up

Using Jupyter Notebooks. Before running `jupter notebook` in the terminal, run the following commands to ensure that you can import pyspark in your notebook:

```
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH" 
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
```
Don't want to save these to our .bashrc file because we typically don't want to first search for the python version located in our spark installation. So we run these two commands each time we plan on using pyspark in our jupyter notebook.

- see [pyspark.md](../05-batch/setup/pyspark.md) for more info on this

Then run `jupyter notebook` and open up a notebook in your browser

## Creating a Spark Session

In your notebook - using [04_pyspark](../05-batch/code/04_pyspark.ipynb)

We first need to import PySpark to our code:

```python
import pyspark
from pyspark.sql import SparkSession
```

We now need to instantiate a ***Spark session***, an object that we use to interact with Spark.

```python
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
```
* `SparkSession` is the class of the object that we instantiate. `builder` is the builder method.
* `master()` sets the Spark _master URL_ to connect to. The `local` string means that Spark will run on a local cluster. `[*]` means that Spark will run with as many CPU cores as possible.
* `appName()` defines the name of our application/session. This will show in the Spark UI.
* `getOrCreate()` will create the session or recover the object if it was previously created.

Once we've instantiated a session, we can access the **Spark UI by browsing to `localhost:4040`** (Spark master). The UI will display all current jobs. Since we've just created the instance, there should be no jobs currently running.

## Reading CSV files

Still using [04_pyspark](../05-batch/code/04_pyspark.ipynb)

Similarlly to Pandas, Spark can read CSV files into ***dataframes***, a tabular data structure. Unlike Pandas, Spark can handle much bigger datasets but it's unable to infer the datatypes of each column.

>Note: Spark dataframes use custom data types; we cannot use regular Python types.

For this example we will use the High Volume For-Hire Vehicle Trip Records for January 2021 available from https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhvhv. The file should be about 720MB in size.

Let's read the file and create a dataframe:

```python
df = spark.read \
    .option("header", "true") \
    .csv('fhvhv_tripdata_2021-01.csv')
```
* `read()` reads the file.
* `option()` contains options for the `read` method. In this case, we're specifying that the first line of the CSV file contains the column names.
* `csv()` is for readinc CSV files.

You can see the contents of the dataframe with `df.show()` (only a few rows will be shown) or `df.head()`. You can also check the current schema with `df.schema`; you will notice that all values are strings.

We can use a trick with Pandas to infer the datatypes:
1. Create a smaller CSV file with the first 1000 records or so.
1. Import Pandas and create a Pandas dataframe. This dataframe will have inferred datatypes.
1. Create a Spark dataframe from the Pandas dataframe and check its schema.
    ```python
    spark.createDataFrame(my_pandas_dataframe).schema
    ```
1. Based on the output of the previous method, import `types` from `pyspark.sql` and create a `StructType` containing a list of the datatypes.
    ```python
    from pyspark.sql import types
    schema = types.StructType([...])
    ```
    * `types` contains all of the available data types for Spark dataframes.
1. Create a new Spark dataframe and include the schema as an option.
    ```python
    df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv('fhvhv_tripdata_2021-01.csv')
    ```

## Partitions

Still using [04_pyspark](../05-batch/code/04_pyspark.ipynb)

A ***Spark cluster*** is composed of multiple ***executors***. Each executor can process data independently in order to parallelize and speed up work.

In the previous example we read a single large CSV file. A file can only be read by a single executor, which means that the code we've written so far isn't parallelized and thus will only be run by a single executor rather than many at the same time.

In order to solve this issue, we can _split a file into multiple parts_ so that each executor can take care of a part and have all executors working simultaneously. These splits are called ***partitions***.

We will now read the CSV file, partition the dataframe and parquetize it. This will create multiple files in parquet format.

>Note: converting to parquet is an expensive operation which may take several minutes.

```python
# create 24 partitions in our dataframe
df = df.repartition(24)
# parquetize and write to fhvhv/2021/01/ folder
df.write.parquet('fhvhv/2021/01/')
```

You may check the Spark UI at any time and see the progress of the current job, which is divided into stages which contain tasks. The tasks in a stage will not start until all tasks on the previous stage are finished.

When creating a dataframe, Spark creates as many partitions as CPU cores available by default, and each partition creates a task. Thus, assuming that the dataframe was initially partitioned into 6 partitions, the `write.parquet()` method will have 2 stages: the first with 6 tasks and the second one with 24 tasks.

Besides the 24 parquet files, you should also see a `_SUCCESS` file which should be empty. This file is created when the job finishes successfully.

Trying to write the files again will output an error because Spark will not write to a non-empty folder. You can force an overwrite with the `mode` argument:

```python
df.write.parquet('fhvhv/2021/01/', mode='overwrite')
```

The opposite of partitioning (joining multiple partitions into a single partition) is called ***coalescing***.

## Spark DataFrames

Still using [04_pyspark](../05-batch/code/04_pyspark.ipynb)

We can create a dataframe from the parquet files we created in the previous section:

```python
df = spark.read.parquet('fhvhv/2021/01/')
```

Unlike CSV files, parquet files contain the schema of the dataset, so there is no need to specify a schema like we previously did when reading the CSV file. You can check the schema like this:

```python
df.printSchema()
```

(One of the reasons why parquet files are smaller than CSV files is because they store the data according to the datatypes, so integer values will take less space than long or string values.)

There are many Pandas-like operations that we can do on Spark dataframes, such as:
* Column selection - returns a dataframe with only the specified columns.
    ```python
    new_df = df.select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID')
    ```
* Filtering by value - returns a dataframe whose records match the condition stated in the filter.
    ```python
    new_df = df.select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID').filter(df.hvfhs_license_num == 'HV0003')
    ```
* And many more. The official Spark documentation website contains a [quick guide for dataframes](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html).
    * e.g. groupby
    * Often easier to do these types of transformations using SQL though - but spark is more flexible, and gives for e.g. UDFs (see below)

## Actions and Transformations

Some Spark methods are "lazy", meaning that they are not executed right away. You can test this with the last instructions we run in the previous section: after running them, the Spark UI will not show any new jobs. However, running `df.show()` right after will execute right away and display the contents of the dataframe; the Spark UI will also show a new job.

These lazy commands are called ***transformations*** and the eager commands are called ***actions***. Computations only happen when actions are triggered.
```python
df.select(...).filter(...).show()
```
```mermaid
graph LR;
    a(df)-->b["select()"]
    b-->c["filter()"]
    c-->d{{"show()"}}
    style a stroke-dasharray: 5
    style d fill:#900, stroke-width:3px
```
Both `select()` and `filter()` are _transformations_, but `show()` is an _action_. The whole instruction gets evaluated only when the `show()` action is triggered.

List of transformations (lazy):
* Selecting columns
* Filtering
* Joins
* Group by
* Partitions
* ...

List of actions (eager):
* Show, take, head
* Write, read
* ...

## Functions and User Defined Functions (UDFs)

Check [04_pyspark](../05-batch/code/04_pyspark.ipynb) for information on / examples of functions and UDFs.


# Preparing Green and Yellow Taxi Data

## Download the Datasets

The [download_data.sh](../05-batch/code/download_data.sh) script downloads the csvs from github.
- Could alternatively use an orchestration tool like Airflow or Prefect to download the data

Added code/data folder to .gitignore so the datasets won't have been pushed to github. Run the scripts again locally if you want the data.

Here is the script:

```bash
#!/bin/bash
set -e

TAXI_TYPE=$1 # "yellow"
YEAR=$2 # 2020

URL_PREFIX="https://s3.amazonaws.com/nyc-tlc/trip+data"

for MONTH in {1..12}; do
  FMONTH=`printf "%02d" ${MONTH}`

  URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv"

  LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
  LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv"
  LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

  echo "donwloading ${URL} to ${LOCAL_PATH}"
  mkdir -p ${LOCAL_PREFIX}
  wget ${URL} -O ${LOCAL_PATH}

  echo "compressing ${LOCAL_PATH}"
  gzip ${LOCAL_PATH}
done
```
* The script loops through 12 months and downloads the dataset for each month for the specified taxi type and year
    * Will do `green` and `yellow` and `2020` and `2021`
    8 Setting them to $1 and $2 means that we can specify the arguments at the command line
* `set -e` means that the script will stop if any of the commands fail. This may happen with `wget` when we download files.
* We parametrize each part of the dataset URL. For the month, we need to convert the month numnber to 2-digits with leading zeros for single-digit months.
* `printf` is a shell built-in command available in bash and other shells which behaves very similar to C's `printf()` function. It can be used instead of `echo` for finer output control.
    * The syntax for the command is `printf [-v var] format [arguments]`
        * The `[-v var]` option is for assigning the output to a variable rather than printing it.
        * `format` is a string that may contain normal characters, backslash-escaped characters and conversion specifications for describing the format.
            * Conversion specifications follow this syntax: `%[flags][width][.precision]specifier`
        * `[arguments]` is a list of arguments of any length that will be passed to the `format` string.
    * `printf "%02d" ${MONTH}` means that the `${MONTH}` argument will be reformatted to show 2 digits with a leading 0 for single digit months.
        * `%` is the conversion specification character.
        * `0` is a flag for padding with leading zeroes.
        * `2` is a width directive; in our case, it means that the output should be of length 2.
        * `d` is the type conversion specifier for signed decimal integers.
    * You may learn more about the `printf` command [in this link](https://linuxize.com/post/bash-printf-command/).
* `mkdir -p` creates both the final directory and its parent directories if they do not exist.
* The `-O` option in `wget ${URL} -O ${LOCAL_PATH}` is for specifying the file name.

**Run the following command to execute the bash script and download the documents:**

```bash
./download_data.sh yellow 2020
```
- the 2021 data for yellow and green only has 7 months (August onwards not available)

After running the script, you may check the final folder structure with `tree`
- might have to run `sudo apt-get install tree` to install tree first

## Parquetise the Datasets

We'll read the csv datasets and apply our defined schemas, and then partition the datasets and parquetize them.
- using same trick as [reading csv section](#reading-csv-files) - creating a pandas df (which infers) datatypes to figure out how to define our schemas (have removed that step from the below ipynb, showing just a finalised step by step to convert the csvs to appropriate parquets)
- You can also set the inferSchema option to True when reading files in Spark, instead of using pandas

See the [05_taxi_schema notebook](../05-batch/code/05_taxi_schema.ipynb) for the steps to do so.

# SQL with Spark

There are other tools available for expressing batch jobs as SQL queries e.g. dbt, Hive, Presto. Spark can also run SQL queries, which can come in handy if you already have a Spark cluster and setting up an additional tool for sporadic use isn't desirable.

## Combining two Datasets

Using [06_spark_sql](../05-batch/code/06_spark_sql.ipynb)

We can now load all of the yellow and green taxi data for 2020 and 2021 to Spark dataframes.

Assuming the parquet files for the datasets are stored on a `data/pq/color/year/month` folder structure:

```python
df_green = spark.read.parquet('data/pq/green/*/*')
df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = spark.read.parquet('data/pq/yellow/*/*')
df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')
```
* Because the pickup and dropoff column names don't match between the 2 datasets, we use the `withColumnRenamed` action to make them have matching names.
* /*/* selects the parquet files from the folders named after each year (2020 and 2021) and each month within each year

We will replicate the [`dm_monthyl_zone_revenue.sql`](../04-analytics-engineering/taxi_rides_ny/models/core/dm_monthly_zone_revenue.sql) model from lesson 4 into Spark. This model makes use of `trips_data`, a combined table of yellow and green taxis, so we will create a combined dataframe with the common columns to both datasets.

We need to find out which are the common columns. We could do this:

```python
set(df_green.columns) & set(df_yellow.columns)
```

However, this command will not respect the column order. We can do this instead to respect the order:

```python
common_colums = []

yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_colums.append(col)
```

Before we combine the datasets, we need to figure out how we will keep track of the taxi type for each record (the `service_type` field in `dm_monthyl_zone_revenue.sql`). We will add the `service_type` column to each dataframe.

```python
from pyspark.sql import functions as F

df_green_sel = df_green \
    .select(common_colums) \
    .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
    .select(common_colums) \
    .withColumn('service_type', F.lit('yellow'))
```
* `F.lit()` adds a _literal_ or constant to a dataframe. We use it here to fill the `service_type` column with a constant value, which is its corresponging taxi type.

Finally, let's combine both datasets:

```python
df_trips_data = df_green_sel.unionAll(df_yellow_sel)
```

We can also count the amount of records per taxi type:

```python
df_trips_data.groupBy('service_type').count().show()
```
* far more yellow rows than green rows

## Querying a Dataset with Temporary Tables

Still using [06_spark_sql](../05-batch/code/06_spark_sql.ipynb)

We can make SQL queries with Spark with `spark.sqll("SELECT * FROM ???")`. SQL expects a table for retrieving records, but a dataframe is not a table, so we need to ***register*** the dataframe as a table first:

```python
df_trips_data.registerTempTable('trips_data')
```
* This method creates a ***temporary table*** with the name `trips_data`.
* ***IMPORTANT***: According to the [official docs](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.registerTempTable.html), this method is deprecated.

With our registered table, we can now perform regular SQL operations.

```python
spark.sql("""
SELECT
    service_type,
    count(1)
FROM
    trips_data
GROUP BY 
    service_type
""").show()
```
* This query outputs the same as `df_trips_data.groupBy('service_type').count().show()`
* Note that the SQL query is wrapped with 3 double quotes (`"`).

The query output can be manipulated as a dataframe, which means that we can perform any queries on our table and manipulate the results with Python as we see fit.

We can now slightly modify the `dm_monthyl_zone_revenue.sql`, and run it as a query with Spark and store the output in a dataframe:

```python
df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")
```
* We removed the `with` statement from the original query because it operates on an external table that Spark does not have access to.
* We removed the `count(tripid) as total_monthly_trips,` line in _Additional calculations_ because it also depends on that external table.
* We change the grouping from field names to references in order to avoid mistakes.

SQL queries are transformations, so we need an action to perform them such as `df_result.show()`.

Once we're happy with the output, we can also store it as a parquet file just like any other dataframe. We could run this:

```python
df_result.write.parquet('data/report/revenue/')
```

However, with our current dataset, this will create more than 200 parquet files of very small size, which isn't very desirable.

In order to reduce the amount of files, we need to reduce the amount of partitions of the dataset, which is done with the `coalesce()` method:

```python
df_result.coalesce(1).write.parquet('data/report/revenue/', mode='overwrite')
```
* This reduces the amount of partitions to just 1.

# Spark Internals

## Spark Clusters

Until now, we've used a ***local cluster*** to run our Spark code, but Spark clusters often contain multiple computers that behave as executors.
- When we set up the spark context we specify the master (using the SparkSession class in our notebooks - where we create our local cluster)

Spark clusters are managed by a ***master***, which behaves similarly to an entry point of a Kubernetes cluster (typically the master will have a web ui on port 4040 that we can use to connect to the master to see what is being executed on the cluster). A ***driver*** (an Airflow DAG, a computer running a local script, etc.) that wants to execute a Spark job will send the job to the master, which in turn will divide the work among the cluster's executors. If any executor fails and becomes offline for any reason, the master will reassign the task to another executor.

```mermaid
flowchart LR;
    a[/"driver (Spark job)"\]--"spark-submit<br/>port 4040"-->master
    subgraph cluster ["Spark cluster"]
    master(["master"])
    master-->e1{{executor}}
    master-->e2{{executor}}
    master-->e3{{executor}}
    end
    subgraph df ["Dataframe (in S3/GCS)"]
    p0[partition]
    e1<-->p1[partition]:::running
    e2<-->p2[partition]:::running
    e3<-->p3[partition]:::running
    p4[partition]
    style p0 fill:#080
    classDef running fill:#b70;
    end
```

Each executor will fetch a ***dataframe partition*** stored in a ***Data Lake*** (usually S3, GCS or a similar cloud provider), do something with it and then store it somewhere, which could be the same Data Lake or somewhere else. If there are more partitions than executors, executors will keep fetching partitions until every single one has been processed.

This is in contrast to [Hadoop](https://hadoop.apache.org/) / HDFS, another data analytics engine, whose executors locally store the data they process. Partitions in Hadoop are duplicated across several executors for redundancy, in case an executor fails for whatever reason (Hadoop is meant for clusters made of commodity hardware computers). However, data locality has become less important as storage and data transfer costs have dramatically decreased and nowadays it's feasible to separate storage from computation, so Hadoop has fallen out of fashion.

## GroupBy in Spark

Using [07_groupby_join](../05-batch/code/07_groupby_join.ipynb)

N.b. remember to create the temptable before running the following sql query

```python
df_green_revenue = spark.sql("""
SELECT 
    date_trunc('hour', lpep_pickup_datetime) AS hour, 
    PULocationID AS zone,

    SUM(total_amount) AS amount,
    COUNT(1) AS number_records
FROM
    green
WHERE
    lpep_pickup_datetime >= '2020-01-01 00:00:00'
GROUP BY
    1, 2  
""")
```

This query will output the total revenue and amount of trips per hour per zone. We need to group by hour and zones in order to do this.

Since the data is split along partitions, it's likely that we will need to group data which is in separate partitions, but executors only deal with individual partitions. Spark solves this issue by separating the grouping in 2 stages:

1. In the first stage, each executor groups the results in the partition they're working on and outputs the results to a temporary partition. These temporary partitions are the ***intermediate results***.

```mermaid
graph LR
    subgraph df [dataframe]
    p1[partition 1]
    p2[partition 2]
    p3[partition 3]
    end
    subgraph ex [executors]
    e1{{executor 1}}
    e2{{executor 2}}
    e3{{executor 3}}
    end
    subgraph ir [intermediate group by]
    i1("hour 1, zone 1, 100 revenue, 5 trips<br/>hour 1, zone 2, 200 revenue, 10 trips")
    i2("hour 1, zone 1, 50 revenue, 2 trips<br/>hour 1, zone 2, 250 revenue, 11 trips")
    i3("hour 1, zone 1, 200 revenue, 10 trips<br/>hour 2, zone 1, 75 revenue, 3 trips")
    end
    p1-->e1
    p2-->e2
    p3-->e3
    e1-->i1
    e2-->i2
    e3-->i3

```

2. The second stage ***shuffles*** the data: Spark will put all records with the ***same keys*** (in this case, the `GROUP BY` keys which are hour and zone) in the ***same partition***. The algorithm to do this is called _external merge sort_. Once the shuffling has finished, we can once again apply the `GROUP BY` to these new partitions and ***reduce*** the records to the ***final output***.
    * Note that the shuffled partitions may contain more than one key, but all records belonging to a key should end up in the same partition.

```mermaid
graph LR
    subgraph IR [intermediate results]
        i1("hour 1, zone 1, 100 revenue, 5 trips<br/>hour 1, zone 2, 200 revenue, 10 trips")
        i2("hour 1, zone 1, 50 revenue, 2 trips<br/>hour 1, zone 2, 250 revenue, 11 trips")
        i3("hour 1, zone 1, 200 revenue, 10 trips<br/>hour 2, zone 1, 75 revenue, 3 trips")
    end
    subgraph F [shuffling]
        f1("hour 1, zone 1, 100 revenue, 5 trips<br/>hour 1, zone 1, 50 revenue, 2 trips<br/>hour 1, zone 1, 200 revenue, 10 trips")
        f2("hour 1, zone 2, 200 revenue, 10 trips<br/>hour 1, zone 2, 250 revenue, 11 trips<br/>hour 2, zone 1, 75 revenue, 3 trips")
    end
    subgraph R ["reduced records - final group by"]
        r1("hour 1, zone 1, 350 revenue, 17 trips")
        r2("hour 1, zone 2, 450 revenue, 21 trips")
        r3("hour 2, zone 1, 75 revenue, 3 trips")
    end
    i1-->f1 & f2
    i2 --> f1 & f2
    i3 --> f1 & f2
    f1-->r1
    f2-->r2 & r3
```

Running the query should display the following DAG in the Spark UI:

```mermaid
flowchart LR
    subgraph S1 [Stage 1]
        direction TB
        t1(Scan parquet)-->t2("WholeStageCodegen(1)")
        t2 --> t3(Exchange)
    end
    subgraph S2 [Stage 2]
        direction TB
        t4(Exchange) -->t5("WholeStageCodegen(2)")
    end
    t3-->t4
```
* The `Exchange` task refers to the shuffling.

If we were to add sorting to the query (adding a `ORDER BY 1,2` at the end), Spark would perform a very similar operation to `GROUP BY` after grouping the data. The resulting DAG would look liked this:

```mermaid
flowchart LR
    subgraph S1 [Stage 1]
        direction TB
        t1(Scan parquet)-->t2("WholeStageCodegen(1)")
        t2 --> t3(Exchange)
    end
    subgraph S2 [Stage 2]
        direction TB
        t4(Exchange) -->t5("WholeStageCodegen(2)")
    end
    subgraph S3 [Stage 3]
        direction TB
        t6(Exchange) -->t7("WholeStageCodegen(3)")
    end
    t3-->t4
    t5-->t6
```

By default, Spark will repartition the dataframe to 200 partitions after shuffling data. For the kind of data we're dealing with in this example this could be counterproductive because of the small size of each partition/file, but for larger datasets this is fine.

Shuffling is an ***expensive operation***, so it's in our best interest to reduce the amount of data to shuffle when querying.
* Keep in mind that repartitioning also involves shuffling data.

## Joins in Spark

Joins in spark operate similarly internally to GroupBys, but there are 2 distinct cases: joining 2 large tables and joining a large table and a small table.

### Joining 2 Large Tables

Using [07_groupby_join](../05-batch/code/07_groupby_join.ipynb)

Creating `df_yellow_revenue` and `df_green_revenue` dfs from the parquet files that we saved to data/report/revenue - in the green and yellow folders - and renaming a couple of the columns (which is a transformation, not an action - so spark won't do anything yet)

```python
df_green_revenue = spark.read.parquet('data/report/revenue/green')
df_yellow_revenue = spark.read.parquet('data/report/revenue/yellow')

df_green_revenue_tmp = df_green_revenue \
    .withColumnRenamed('amount', 'green_amount') \
    .withColumnRenamed('number_records', 'green_number_records')

df_yellow_revenue_tmp = df_yellow_revenue \
    .withColumnRenamed('amount', 'yellow_amount') \
    .withColumnRenamed('number_records', 'yellow_number_records')
```

Now using an [outer join](https://dataschool.com/how-to-teach-people-sql/sql-join-types-explained-visually/) to display the amount of trips and revenue per hour per zone (the groups as per the group by section above) for green and yellow taxis at the same time regardless of whether the hour/zone combo had one type of taxi trips or the other:

```python
df_join = df_green_revenue_tmp.join(df_yellow_revenue_tmp, on=['hour', 'zone'], how='outer')
```
* `on=` receives a list of columns by which we will join the tables. This will result in a ***primary composite key*** for the resulting table.
* `how=` specifies the type of `JOIN` to execute.

An outer join = includes all data from both tables (so if there is no matching row in the other table for 'on' dimensions, there will be Nulls instead). See below result of `df_join.show()`

![df_join](images/05_01.png)

```mermaid
graph LR
    subgraph S1[Stage 1]
        direction TB
        s1(Scan parquet)-->s2("WholeStageCodegen(3)")-->s3(Exchange)
    end
    subgraph S2[Stage 2]
        direction TB
        s4(Scan parquet)-->s5("WholeStageCodegen(1)")-->s6(Exchange)
    end
    subgraph S3[Stage 3]
        direction TB
        s7(Exchange)-->s8("WholeStageCodegen(2)")
        s9(Exchange)-->s10("WholeStageCodegen(4)")
        s8 & s10 -->s11(SortMergeJoin)-->s12("WholeStageCodegen(5)")
    end
    s3-->s9
    s6-->s7
```

Stages 1 and 2 belong to the creation of `df_green_revenue_tmp` and `df_yellow_revenue_tmp`.

For stage 3, given all records for yellow taxis `Y1, Y2, ... , Yn` and for green taxis `G1, G2, ... , Gn` and knowing that the resulting composite key is `key K = (hour H, zone Z)`, we can express the resulting complex records as `(Kn, Yn)` for yellow records and `(Kn, Gn)` for green records. Spark will first ***shuffle*** the data like it did for grouping (using the ***external merge sort algorithm***) and then it will ***reduce*** the records by joining yellow and green data for matching keys to show the final output.

```mermaid
graph LR
    subgraph Y [yellow taxis]
        y1("(K1, Y1)<br/>(K2, Y2)")
        y2("(K3, Y3)")
    end
    subgraph G [green taxis]
        g1("(K2, G1)<br/>(K3, G2)")
        g2("(K4, G3)")
    end
    subgraph S [shuffled partitions]
        s1("(K1, Y1)<br/>(K4, G3)")
        s2("(K2, Y2)<br/>(K2, G1)")
        s3("(K3, Y3)<br/>(K3, G2)")
    end
    subgraph R [reduced partitions]
        r1("(K1, Y1, Ø)<br/>(K4, Ø, G3)")
        r2("(K2, Y2, G1)")
        r3("(K3, Y3, G2)")
    end
    y1 --> s1 & s2
    y2 --> s3
    g1 --> s2 & s3
    g2 --> s1
    s1 --> r1
    s2 --> r2
    s3 --> r3
```
* Because we're doing an ***outer join***, keys which only have yellow taxi or green taxi records will be shown with empty fields for the missing data, whereas keys with both types of records will show both yellow and green taxi data.
    * If we did an ***inner join*** instead, the records such as `(K1, Y1, Ø)` and `(K4, Ø, G3)` would be excluded from the final result.


### Joining a large table and a small table


Using the [zones lookup table](../05-batch/code/data/zones/) to match each zone ID in df_join with it's corresponding zone name

```python
df_zones = spark.read.parquet('zones/')

df_result = df_join.join(df_zones, df_join.zone == df_zones.LocationID)

df_result.drop('LocationID', 'zone').write.parquet('tmp/revenue-zones')
```
* The default join type in Spark SQL is the inner join.
* Because we renamed the `LocationID` in the joint table to `zone`, we can't simply specify the columns to join and we need to provide a condition as criteria.
* We use the `drop()` method to get rid of the extra columns we don't need anymore, because we only want to keep the zone names and both `LocationID` and `zone` are duplicate columns with numeral ID's only.
* We also use `write()` instead of `show()` because `show()` might not process all of the data.

The `zones` table is actually very small and joining both tables with merge sort is unnecessary. What Spark does instead is ***broadcasting***: Spark sends a copy of the complete table to all of the executors and each executor then joins each partition of the big table in memory by performing a lookup on the local broadcasted table.

```mermaid
graph LR
    subgraph B [big table]
        b1[partition 1]
        b2[partition 2]
        b3[partition 3]
    end
    subgraph E [executors]
        subgraph E1 [executor 1]
            e1{{executor}} -.->|lookup| z1["zones (local)"]
            z1 -.->|return| e1
        end
        subgraph E2 [executor 2]
            e2{{executor}} -.->|lookup| z2["zones (local)"]
            z2 -.->|return| e2
        end
        subgraph E3 [executor 3]
            e3{{executor}} -.->|lookup| z3["zones (local)"]
            z3 -.->|return| e3
        end
    end
    subgraph R [result]
        r1[zone, ...]
        r2[zone, ...]
        r3[zone, ...]
    end
    z[zones]-.->|broadcast| z1 & z2 & z3
    b1-->e1-->r1
    b2-->e2-->r2
    b3-->e3-->r3
```

Shuffling isn't needed because each executor already has all of the necessary info to perform the join on each partition, thus speeding up the join operation by orders of magnitude.

# Resilient Distributed Datasets

Using [08_rdds.ipynb notebook](../05-batch/code/08_rdds.ipynb)

RDDs are the basis for what Spark uses to do distributed configurations.

***Resilient Distributed Datasets*** (RDDs) are the main abstraction provided by Spark and consist of collection of elements partitioned accross the nodes of the cluster.

Dataframes are a layer of abstraction on top of RDDs and contain a schema as well, which plain RDDs do not - they are just a distributed collection of objects

## RDDs: Map & Reduce

### From Dataframe to RDD

Spark dataframes contain a `rdd` field which contains the raw RDD of the dataframe. The RDD's objects used for the dataframe are called ***rows***.

If we look at the SQL query we saw in the [GROUP BY section](#group-by-in-spark), which we perfomed on `05-batch/code/data/pq/green/`  saved as parquet files in `05-batch/code/data/report/revenue/green/`

```sql
SELECT 
    date_trunc('hour', lpep_pickup_datetime) AS hour, 
    PULocationID AS zone,

    SUM(total_amount) AS amount,
    COUNT(1) AS number_records
FROM
    green
WHERE
    lpep_pickup_datetime >= '2020-01-01 00:00:00'
GROUP BY
    1, 2
```

We can reimplement this query with RDD's instead:

1. `SELECT` 3 fields from the RDDs rows
    ```python
    rdd = df_green \
        .select('lpep_pickup_datetime', 'PULocationID', 'total_amount') \
        .rdd
    ```
1. Recreate the `WHERE` clause by using the `filter()` and `take()` methods:
    * `filter()` returns a new RDD cointaining only the elements that satisfy a _predicate_, which in our case is a function that we pass as a parameter.
    * `take()` takes as many elements from the RDD as stated - each element is a row of data (so just keeping one row below)
    ```python
    from datetime import datetime

    start = datetime(year=2020, month=1, day=1)

    def filter_outliers(row):
        return row.lpep_pickup_datetime >= start

    rdd.filter(filter_outliers).take(1)
    ```
### Operations on RDDs: map, filter, reduceByKey

> Note: the `Group By` is more complex and makes use of special methods 

1. We need to generate _intermediate results_ in a very similar way to the original SQL query, so we will need to create the _composite key_ `(hour, zone)` and a _composite value_ `(amount, count)`, which are the 2 halves of each record that the executors will generate. Once we have a function that generates the record, we will use the `map()` method, which takes an RDD, transforms it with a function (our key-value function) and returns a new RDD.
    ```python
    def prepare_for_grouping(row): 
        hour = row.lpep_pickup_datetime.replace(minute=0, second=0, microsecond=0)
        zone = row.PULocationID
        key = (hour, zone)
        
        amount = row.total_amount
        count = 1
        value = (amount, count)

        return (key, value)
    

    rdd \
        .filter(filter_outliers) \
        .map(prepare_for_grouping) # so it maps key and value for each row in the rdd
    ```

1. We now need to use the `reduceByKey()` method, which will take all records with the same key and puts them together in a single record by transforming all the different values according to some rules which we can define with a custom function. Since we want to count the total amount and the total number of records, we just need to add the values:
    ```python
    # we get 2 value tuples from 2 separate records as input
    def calculate_revenue(left_value, right_value):
        # tuple unpacking
        left_amount, left_count = left_value
        right_amount, right_count = right_value
        
        output_amount = left_amount + right_amount
        output_count = left_count + right_count
        
        return (output_amount, output_count)
    
    rdd \
        .filter(filter_outliers) \
        .map(prepare_for_grouping) \
        .reduceByKey(calculate_revenue)

* idea is that the reduceByKey function goes along the tuples with the same key in pairs e.g. (k1, 2), (k1, 4) reduced to (k1, 6) and then (k1, 6) paired with a 3rd tuple (k1, 3), which is reduced to (k1, 9) and so on...
* As the rows in our example have two value records, will be summing both amount and count for each tuple pair

1. The output we have is already usable but not very nice, so we map the output again in order to _unwrap_ it.
    ```python
    from collections import namedtuple
    RevenueRow = namedtuple('RevenueRow', ['hour', 'zone', 'revenue', 'count'])
    def unwrap(row):
        return RevenueRow(
            hour=row[0][0], 
            zone=row[0][1],
            revenue=row[1][0],
            count=row[1][1]
        )

    rdd \
        .filter(filter_outliers) \
        .map(prepare_for_grouping) \
        .reduceByKey(calculate_revenue) \
        .map(unwrap)
    ```
    * Using `namedtuple` isn't necessary but it will help in the next step.

### From RDD to DataFrame

Can then take the resulting RDD and convert it to a dataframe with `toDF()`. We will need to generate a schema first because we lost it when converting RDDs:

```python
from pyspark.sql import types

result_schema = types.StructType([
    types.StructField('hour', types.TimestampType(), True),
    types.StructField('zone', types.IntegerType(), True),
    types.StructField('revenue', types.DoubleType(), True),
    types.StructField('count', types.IntegerType(), True)
])

df_result = rdd \
    .filter(filter_outliers) \
    .map(prepare_for_grouping) \
    .reduceByKey(calculate_revenue) \
    .map(unwrap) \
    .toDF(result_schema) 
```
* We can use `toDF()` without any schema as an input parameter, but Spark will have to figure out the schema by itself which may take a substantial amount of time. Using `namedtuple` in the previous step allows Spark to infer the column names but Spark will still need to figure out the data types; by passing a schema as a parameter we skip this step and get the output much faster.

As you can see, manipulating RDDs to perform SQL-like queries is complex and time-consuming. Ever since Spark added support for dataframes and SQL, manipulating RDDs in this fashion has become obsolete, but since dataframes are built on top of RDDs, knowing how they work can help us understand how to make better use of Spark.

## Spark RDD mapPartitions

The `mapPartitions()` function behaves similarly to `map()` in how it receives an RDD as input and transforms it into another RDD with a function that we define but it transforms partitions rather than elements. In other words: `map()` creates a new RDD by transforming every single element, whereas `mapPartitions()` transforms every partition to create a new RDD.

`mapPartitions()` is a convenient method for dealing with large datasets because it allows us to separate it into chunks that we can process more easily, which is handy for workflows such as Machine Learning.

### Using `mapPartitions()` for ML

Let's assume we want to predict taxi travel duration with the green taxi dataset. We will use `VendorID`, `lpep_pickup_datetime`, `PULocationID`, `DOLocationID` and `trip_distance` as our features. The below creates an RDD with these columns:

```python
columns = ['VendorID', 'lpep_pickup_datetime', 'PULocationID', 'DOLocationID', 'trip_distance']

duration_rdd = df_green \
    .select(columns) \
    .rdd
```

Let's now create the method that `mapPartitions()` will use to transform the partitions. This method will essentially call our prediction model on the partition that we're transforming:

```python
import pandas as pd

def model_predict(df):
    # ML code goes here
    (...)
    # predictions is a Pandas dataframe with the field predicted_duration in it
    return predictions

# this function is applied to mapPartitions method on an rdd
def apply_model_in_batch(rows):
    df = pd.DataFrame(rows, columns=columns)
    predictions = model_predict(df)
    df['predicted_duration'] = predictions

    for row in df.itertuples():
        yield row
```

* We're assuming that our model works with Pandas dataframes, so we need to import the library.
* We are converting the input partition into a dataframe for the model.
    * RDD's do not contain column info, so we use the `columns` param to name the columns because our model may need them.
    * Pandas will crash if the dataframe is too large for memory! We're assuming that this is not the case here, but you may have to take this into account when dealing with large partitions. You can use the [itertools package](https://docs.python.org/3/library/itertools.html) for slicing the partitions before converting them to dataframes.
* Our model will return another Pandas dataframe with a `predicted_duration` column containing the model predictions.
* `df.itertuples()` is an iterable that returns a tuple containing all the values in a single row, for all rows. Thus, `row` will contain a tuple with all the values for a single row.
* `yield` is a Python keyword that behaves similarly to `return` but returns a ***generator object*** instead of a value. This means that a function that uses `yield` can be iterated on. Spark makes use of the generator object in `mapPartitions()` to build the output RDD.
  * You can learn more about the `yield` keyword [in this link](https://realpython.com/introduction-to-python-generators/).

With our defined fuction, we are now ready to use `mapPartitions()` and run our prediction model on our full RDD:

```python
df_predicts = duration_rdd \
    .mapPartitions(apply_model_in_batch)\
    .toDF() \
    .drop('Index')

df_predicts.select('predicted_duration').show()
```

* We're not specifying the schema when creating the dataframe, so it may take some time to compute.
* We drop the `Index` field because it was created by Spark and it is not needed.

As a final thought, you may have noticed that the `apply_model_in_batch()` method does NOT operate on single elements, but rather it takes the whole partition and does something with it (in our case, calling a ML model). If you need to operate on individual elements then you're better off with `map()`.

# Running Spark in the Cloud

So far we've seen how to run Spark locally and how to work with local data. In this section we will cover how to use Spark with remote data and run Spark in the cloud as well.

## Connecting to Google Cloud Storage

Google Cloud Storage is an _object store_, which means that it doesn't offer a fully featured file system. Spark can connect to remote object stores by using ***connectors***; each object store has its own connector, so we will need to use [Google's Cloud Storage Connector](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage) if we want our local Spark instance to connect to our Data Lake.

Before we do that, we will use `gsutil` to upload our local files to our Data Lake. `gsutil` is included with the GCP SDK, so you should already have it if you've followed the previous chapters.

### Uploading files to Cloud Storage with `gsutil`

Will be uploading to this bucket: `dtc_data_lake_evident-display-410312` in a new folder called `pq`

The data we want to upload was some of the data created in this weeks lessons: `../05-batch/code/data/pq`

Assuming you've got a bunch of parquet files you'd like to upload to Cloud Storage, run the following command to upload them:

```bash
gsutil -m cp -r 05-batch/code/data/pq gs://dtc_data_lake_evident-display-410312/pq
```
* The `-m` option is for enabling multithreaded upload in order to speed it up.
* `cp` is for copying files.
* `-r` stands for _recursive_; it's used to state that the contents of the local folder are to be uploaded. For single files this option isn't needed.

### Configuring Spark with the GCS connector

Go to the [Google's Cloud Storage Connector page](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage) and download the corresponding version of the connector. The version tested for this lesson is version 2.5.5 for Hadoop 3; create a `lib` folder (made this in 05-batch folder) in your work directory and run the following command from it:

```bash
gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar gcs-connector-hadoop3-2.2.5.jar
```

This will download the connector to the local folder.

We now need to follow a few extra steps before creating the Spark session in our notebook. Import the following libraries:

```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
```

Now we need to configure Spark by creating a configuration object. Run the following code to create it:

```python
credentials_location = '/home/USERNAME/.gc/evident-display-410312-6d17d29a1ecf.json'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "../lib/gcs-connector-hadoop3-2.2.5.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)
```

You may have noticed that we're including a couple of options that we previously used when creating a Spark Session with its builder. That's because we implicitly created a ***context***, which represents a connection to a spark cluster. This time we need to explicitly create and configure the context like so:

```python
sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
```

This will likely output a warning when running the code. You may ignore it.

We can now finally instantiate a Spark session:

```python
spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()
```

### Reading the remote data

In order to read the parquet files stored in the Data Lake, you simply use the bucket URI as a parameter, like so:

```python
df_green = spark.read.parquet('gs://dtc_data_lake_evident-display-410312/pq/green/*/*')
```

You should obviously change the URI in this example for yours.

You may now work with the `df_green` dataframe normally.

## Creating a Local Spark Cluster

Using [10_local_cluster](../05-batch/code/10_local_cluster.ipynb)

### Spark standalone master and workers

Have so far been creating Spark sessions from notebooks, using the following

```python
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
```

This code will stard a local cluster, but once the notebook kernel is shut down, the cluster will disappear.

We will now see how to crate a Spark cluster in [Standalone Mode](https://spark.apache.org/docs/latest/spark-standalone.html) so that the cluster can remain running even after we stop running our notebooks.

Simply go to your Spark install directory (/spark/spark-3.4.2-bin-hadoop3/ for me) from a terminal and run the following command:

```bash
./sbin/start-master.sh
```

fyi, following command will give Spark install directory as we saved the variable in .bashrc earlier on:

```bash
 echo $SPARK_HOME
 ```

 You should now be able to open the main Spark dashboard by browsing to `localhost:8080` (remember to forward the port if you're running it on a virtual machine). At the very top of the dashboard the URL for the dashboard should appear; copy it and use it in your session code like so:

```python
spark = SparkSession.builder \
    .master("spark://<URL>:7077") \
    .appName('test') \
    .getOrCreate()
```
* Note that we used the HTTP port 8080 for browsing to the dashboard but we use the Spark port 7077 for connecting our code to the cluster.
* Using `localhost` as a stand-in for the URL (which appears in the spark master ui at localhost:8080 - was `spark://de-zoomcamp.europe-west2-c.c.evident-display-410312.internal:7077` for me) may not work.
* You'll now see an application is spark master of name `test`

You may note that in the Spark dashboard there aren't any _workers_ listed. The actual Spark jobs are run from within ***workers*** (or _slaves_ in older Spark versions), which we need to create and set up.

Similarly to how we created the Spark dashboard, we can run a worker from the command line by running the following command from the Spark install directory:

```bash
./sbin/start-worker.sh spark://de-zoomcamp.europe-west2-c.c.evident-display-410312.internal:7077
```
* In older Spark versions, the script to run is `start-slave.sh` .

Once you've run the command, you should see a worker in the Spark dashboard - and you can now execute actions

Note that a worker may not be able to run multiple jobs simultaneously. If you're running separate notebooks and connecting to the same Spark worker, you can check in the Spark dashboard how many Running Applications exist. Since we haven't configured the workers, any jobs will take as many resources as there are available for the job.

### Parametrizing our scripts for Spark

As spark is now running independently of our notebook, we can use spark with a .py file.

You can convert a notebook into a .py script using `nbconvert`:

```bash
jupyter nbconvert --to=script 06_spark_sql.ipynb
```

N.b. can see the converted and parameterized script [here](../05-batch/code/06_spark_sql.py)
- suitable for running the spark-submit command with (as the spark master url has been taken out of this script)

So far we've hard-coded many of the values such as folders and dates in our code, but with a little bit of tweaking we can make our code so that it can receive parameters from Spark and make our code much more reusable and versatile. We will use the [argparse library](https://docs.python.org/3/library/argparse.html) for parsing parameters. 

```python
import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output
```

We can now modify previous lines using the 3 parameters we've created. For example:

```python
df_green = spark.read.parquet(input_green)
```

Once we've finished our script, we simply call it from a terminal line with the parameters we need:

```bash
python 06_spark_sql.py \
    --input_green=data/pq/green/2020/*/ \
    --input_yellow=data/pq/yellow/2020/*/ \
    --output=data/report-2020
```

### Submitting Spark jobs with Spark submit

However, we still haven't covered any Spark specific parameters; things like the the cluster URL when having multiple available clusters or how many workers to use for the job. Instead of specifying these parameters when setting up the session inside the script, we can use an external script called [Spark submit](https://spark.apache.org/docs/latest/submitting-applications.html).

The basic usage is as follows:

```bash
spark-submit \
    --master="spark://de-zoomcamp.europe-west2-c.c.evident-display-410312.internal:7077" \
    06_spark_sql.py \
        --input_green=data/pq/green/2020/*/ \
        --input_yellow=data/pq/yellow/2020/*/ \
        --output=data/report-2020
```
* You can specify other parameters e.g. how many executors to use

And the Spark session code in the script is simplified like so:

```python
spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()
```

You may find more sophisticated uses of `spark-submit` in the [official documentation](https://spark.apache.org/docs/latest/submitting-applications.html).

After you're done running Spark in standalone mode, you will need to manually shut it down. Simply run the `./sbin/stop-worker.sh` (`./sbin/stop-slave.sh` in older Spark versions) and `./sbin/stop-master.sh` scripts to shut down Spark.


## Setting up a Dataproc Cluster

### Creating the cluster

[Dataproc](https://cloud.google.com/dataproc) is Google's cloud-managed service for running Spark and other data processing tools such as Flink, Presto, etc.

You may access Dataproc from the GCP dashboard and typing `dataproc` on the search bar. The first time you access it you will have to enable the API.

In the images below you may find some example values for creating a simple cluster. Give it a name of your choosing and choose the same region as your bucket. We would normally choose a `standard` cluster, but you may choose `single node` if you just want to experiment and not run any jobs.

![creating a cluster](images/05_02.png)

You can optionally install additional components but we won't be covering them in this lesson.

![creating a cluster](images/05_03.png)

You can leave all other optional settings with their default values. After you click on `Create`, it will take a few seconds to create the cluster. You may notice an extra VM instance under VMs; that's the Spark instance.

### Running a job with the web UI

In a [previous section](#configuring-spark-with-the-gcs-connector) we saw how to connect Spark to our bucket in GCP. However, in Dataproc we don't need to specify this connection because it's already pre-comfigured for us. We will also submit jobs using a menu, following similar principles to what we saw in the previous section.

In Dataproc's _Clusters_ page, choose your cluster and on the _Cluster details_ page, click on `Submit job`. Under _Job type_ choose `PySpark`, then in _Main Python file_ write the path to your script.

You'll need to write the script to your google cloud bucket first - and then use that path in the job config section in the image below (n.b. the path to the python file in the image is wrong, the bucket name is `tc_data_lake_evident-display-410312`):

```bash
gsutil cp 06_spark_sql.py gs://<bucketname/folder/06_spark_sql.py>
```

![setting up a job](images/05_04.png)

Make sure that your script does not specify the `master` cluster! Your script should take the connection details from Dataproc; make sure it looks something like this:

```python
spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()
```


We also need to specify arguments, in a similar fashion to what we saw [in the previous section](#parametrizing-our-scripts-for-spark), but using the URL's for our folders rather than the local paths:

![setting up a job](images/05_05.png)

Now press `Submit`. Sadly there is no easy way to access the Spark dashboard (may have to enter the spark cluster VM that was created, in a similar way I'm using a gcp VM as the environment for this course) but you can check the status of the job from the `Job details` page.

### Running a job with the gcloud SDK

Besides the web UI, there are additional ways to run a job, listed [in this link](https://cloud.google.com/dataproc/docs/guides/submit-job) e.g. using REST. We will focus on the gcloud SDK now.

Before you can submit jobs with the SDK, you will need to grant permissions to the Service Account we've been using so far. Go to _IAM & Admin_ and edit your Service Account so that the `Dataproc Administrator` role is added to it.

We can now submit a job from the command line, like this:

```bash
gcloud dataproc jobs submit pyspark \
    --cluster=<your-cluster-name> \
    --region=europe-west6 \
    gs://<url-of-your-script> \
    -- \
        --param1=<your-param-value> \
        --param2=<your-param-value>
```

For me, this is:

```bash
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=europe-west2 \
    gs://dtc_data_lake_evident-display-410312/code/06_spark_sql.py \
    -- \
        --input_green=gs://dtc_data_lake_evident-display-410312/pq/green/2021/*/ \
        --input_yellow=gs://dtc_data_lake_evident-display-410312/pq/yellow/2021/*/ \
        --output=gs://dtc_data_lake_evident-display-410312/report-2021
```

## Connecting Spark to BigQuery

This [link](https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example#pyspark) talks about
connecting Spark to BigQuery.

Using [06_spark_sql_biq_query.py](../05-batch/code/06_spark_sql_big_query.py) - which is a copy of `06_spark_sql_big_query` apart from a couple of adjustments.

Remove the last line writing to parquet and replace with:

```python
df_result.write.format('bigquery') \
    .option('table', output) \
    .save()
```

- Will write the data to this table in the trips_data_all schema in BigQuery - the table doesn't exist in BigQuery, Spark should create this for us: `trips_data_all.reports-2020`. This is our `output` parameter.

We also need to specify the name of a temporary bucket created by Dataproc (this is created when you create the cluster), as the spark-bigquery connector writes the data to BigQuery by first buffering all the data into a Cloud Storage temporary table, and then copies all the data into BigQuery in one operation (the connector will delete all the temp files once the BigQuery load operation has succeeded): 

```python
bucket = "dataproc-temp-europe-west2-603267822548-sfs4menx"
spark.conf.set('temporaryGcsBucket', bucket)
```

We can now upload the script to our GCS bucket:

```bash
gsutil cp 06_spark_sql_big_query.py gs://dtc_data_lake_evident-display-410312/code/06_spark_sql_big_query.py 
```


The code to execute in the command line:

```bash
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=europe-west2 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://dtc_data_lake_evident-display-410312/code/06_spark_sql_big_query.py \
    -- \
        --input_green=gs://dtc_data_lake_evident-display-410312/pq/green/2020/*/ \
        --input_yellow=gs://dtc_data_lake_evident-display-410312/pq/yellow/2020/*/ \
        --output=trips_data_all.reports-2020
```

In BigQuery, a `reports-2020` table exists in the `trips_data_all`, and you can run this query to check:

``` sql
SELECT * FROM `evident-display-410312.trips_data_all.reports-2020` LIMIT 10;
```



