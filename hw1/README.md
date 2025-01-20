Q1:
(base) almaz@Almazyans-MacBook-Pro hw1 % docker run -it --entrypoint=bash python:3.12.8
root@6f2078056fc7:/# pip --version
pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)
-------------------------

Q2:
postgres:5432
-------------------------

Q3:
SQL script:
select
	count(case when trip_distance <= 1 then 1 end) as up_to_1_mile,
	count(case when trip_distance > 1 and trip_distance <= 3 then 1 end) as between_1_3_miles,
	count(case when trip_distance > 3 and trip_distance <= 7 then 1 end) as between_3_7_miles,
	count(case when trip_distance > 7 and trip_distance <= 10 then 1 end) as between_7_10_miles,
	count(case when trip_distance > 10 then 1 end) as over_10_miles
from green_taxi_data
where
	date(lpep_pickup_datetime) between '2019-10-01' and '2019-10-31'
	and date(lpep_dropoff_datetime) between '2019-10-01' and '2019-10-31'

Answer: 104,802; 198,924; 109,603; 27,678; 35,189
-------------------------

Q4:
SQL script:
with cte as 
	(select
		max(trip_distance) as max_trip_distance 
	from green_taxi_data
	where
		date(lpep_pickup_datetime) between '2019-10-01' and '2019-10-31'
		and date(lpep_dropoff_datetime) between '2019-10-01' and '2019-10-31')

select
	date(lpep_pickup_datetime)
from green_taxi_data a join cte b on a.trip_distance = b.max_trip_distance
where
	date(lpep_pickup_datetime) between '2019-10-01' and '2019-10-31'
	and date(lpep_dropoff_datetime) between '2019-10-01' and '2019-10-31'
group by 1

Answer: 2019-10-11
-------------------------

Q5:
SQL script:
select
	array_to_string(array(
		select
			b."Zone"
		from
			green_taxi_data a join zones b on a."PULocationID" = b."LocationID"
		where
			date(lpep_pickup_datetime) = '2019-10-18'
		group by 1
		having sum(total_amount) > 13000 LIMIT 3), ', ') as three_biggest_pickup_zones

Answer: East Harlem North, East Harlem South, Morningside Heights
-------------------------

Q6:
SQL script:
select
	b."Zone"
from
	green_taxi_data a
		join zones b on a."DOLocationID" = b."LocationID"
where
	date(lpep_pickup_datetime) between '2019-10-01' and '2019-10-31'
	and date(lpep_dropoff_datetime) between '2019-10-01' and '2019-10-31'
	and a."PULocationID" in (select "LocationID" from zones where "Zone" = 'East Harlem North')
	and a.tip_amount = 
		(select max(tip_amount) from green_taxi_data a 
		where date(lpep_pickup_datetime) between '2019-10-01' and '2019-10-31'
		and date(lpep_dropoff_datetime) between '2019-10-01' and '2019-10-31'
		and a."PULocationID" in (select "LocationID" from zones where "Zone" = 'East Harlem North'))
group by 1
limit 1

Answer: JFK Airport
-------------------------

Q7:
-------------------------

