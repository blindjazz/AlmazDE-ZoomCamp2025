### HomeWork Week #4
-----------

- Question #1: Understanding dbt model resolution

export has much more power than env
took answer here: https://docs.getdbt.com/reference/dbt-jinja-functions/env_var

> Answer: select * from myproject.my_nyc_tripdata.ext_green_taxi

-----------

- Question #2: dbt Variables & Dynamic Models

var --> env_var --> default
took answer here: https://docs.getdbt.com/reference/dbt-jinja-functions/env_var

> Answer: Update the WHERE clause to pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY

-----------

- Question #3: dbt Data Lineage and Execution

I think I thought I guess 

> Answer: dbt run --select models/staging/+

-----------

- Question #4: dbt Macros and Jinja

> Answer: All ok, except - *Setting a value for DBT_BIGQUERY_STAGING_DATASET env var is mandatory, or it'll fail to compile*

-----------
- Question #5: Taxi Quarterly Revenue Growth

with cte as (
    select
        extract(year from pickup_datetime) as year,
        extract(quarter from pickup_datetime) as quarter,
        concat(extract(year from pickup_datetime),'/Q',extract(quarter from pickup_datetime)) as year_quarter,
        extract(month from pickup_datetime) as month,
        a.* 
    from
        prod.fact_trips a),
y_2019 as
    (select 
        service_type,
        year_quarter,
        quarter,
        sum(total_amount) as total_amount
    from
        cte
    where
        year = 2019
    group by 1,2,3),
y_2020 as
    (select 
        service_type,
        year_quarter,
        quarter,
        sum(total_amount) as total_amount
    from
        cte
    where
        year = 2020
    group by 1,2,3)

select
    a.service_type,
    a.year_quarter,
    b.year_quarter,
    a.total_amount/b.total_amount as diff_1,
    b.total_amount/a.total_amount as diff_2,
    a.total_amount-b.total_amount as diff_3,
    a.total_amount,
    b.total_amount
from
    y_2020 a join y_2019 b
    on a.service_type = b.service_type and a.quarter = b.quarter
order by 1,2


> Answer: green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}

-----------

- Question #6: P97/P95/P90 Taxi Monthly Fare

with cte as 
    (select
        service_type,
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month,
        fare_amount
    from
        prod.fact_trips a
    where
        fare_amount > 0 and trip_distance > 0
        and payment_type_description in ('Cash', 'Credit card'))

select distinct
    service_type,
    year,month,
    percentile_cont(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) AS p97,
    percentile_cont(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) AS p95,
    percentile_cont(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) AS p90
from
    cte
where
  year = 2020 and month = 4
order by 1,2,3

> Answer: green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}

-----------

- Question #7: Top #Nth longest P90 travel time Location for FHV

with cte as (
select
    extract(year from pickup_datetime) as year,
    extract(month from pickup_datetime) as month,
    TIMESTAMP_DIFF(dropOff_datetime, pickup_datetime, second) as trip_duration,
    fhv_tripdata.*,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone
from
    trip_data_all.fhv_tripdata
    inner join prod.dim_zones as pickup_zone 
        on fhv_tripdata.PUlocationID = pickup_zone.locationid
    inner join prod.dim_zones as dropoff_zone
        on fhv_tripdata.DOlocationID = dropoff_zone.locationid
where
    dispatching_base_num is not null
),
final as
(select 
    *,
    percentile_cont(trip_duration, 0.90) OVER (PARTITION BY year, month, PUlocationID, DOlocationID) AS p90
from cte)

select distinct
    dropoff_zone, p90
from
    final
where
    month = 11
    and year = 2019
    and pickup_zone = 'Yorkville East' -- 'SoHo' -- 'Newark Airport'
order by 2 desc
limit 2

> Answer: LaGuardia Airport, Chinatown, Garment District

-----------