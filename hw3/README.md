### HomeWork Week #3
-----------
- Create an external table using the Yellow Taxi Trip Records.

create or replace external table `hw3.zoomcamp.external_yellow_tripdata`
options (
    format = 'PARQUET',
    uris = ['gs://kestra/yellow_tripdata_2024-*.parquet'] );
-----------
- Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table).

create or replace table `hw3.zoomcamp.yellow_tripdata` as
select * from `hw3.zoomcamp.external_yellow_tripdata`;
-----------
- Question #1: What is count of records for the 2024 Yellow Taxi Data?

select count(*) from `hw3.zoomcamp.external_yellow_tripdata`;

> Answer: 20332093
-----------
- Question #2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

select count(distinct PULocationID) from `hw3.zoomcamp.yellow_tripdata`;
select count(distinct PULocationID) from `hw3.zoomcamp.external_yellow_tripdata`

> Answer: 0 MB for the External Table and 155.12 MB for the Materialized Table
-----------
- Question #3: Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

select PULocationID from `hw3.zoomcamp.yellow_tripdata`;
select PULocationID, DOLocationID from `hw3.zoomcamp.yellow_tripdata`;

> Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
-----------
- Question #4: How many records have a fare_amount of 0?

select count(*) from `hw3.zoomcamp.yellow_tripdata` where fare_amount = 0

> Answer: 8333
-----------
- Question #5: What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

create or replace table `hw3.zoomcamp.yellow_tripdata_part_clus`
    partition by date(tpep_dropoff_datetime)
    cluster by VendorID as
    (select * from `hw3.zoomcamp.yellow_tripdata`);

> Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID
-----------
- Question #6: Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive). Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

select distinct(vendorId) from `hw3.zoomcamp.yellow_tripdata` where date(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15'

select distinct(vendorId) from `hw3.zoomcamp.yellow_tripdata_part_clus` where date(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15'

> Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
-----------
- Question #7: Where is the data stored in the External Table you created?

> Answer: GCP Bucket
-----------
- Question #8: It is best practice in Big Query to always cluster your data:

> Answer: True


