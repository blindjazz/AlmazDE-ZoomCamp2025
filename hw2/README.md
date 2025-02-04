### HomeWork Week #2

- QUESTION #1

used kestra/postgres_taxi.yaml file in Kestra, commented purge_files Task...

*id: purge_files*<br>
*type: io.kestra.plugin.core.storage.PurgeCurrentExecutionFiles*<br>
*description: This will remove output files. If you'd like to explore Kestra outputs, disable it.*

...and ran yellow_tripdata_2020-12 CSV loading

![q1 answer](pics/q1.png)

> ANSWER: 128.3 MB

--------------------

- QUESTION #2

Here's where I hope the answer is right.

> ANSWER: green_tripdata_2020-04.csv

--------------------

- QUESTION #3

  `SELECT count(*) FROM public.yellow_tripdata where filename like 'yellow_tripdata_2020%'`

> ANSWER: 24,648,499

--------------------

- QUESTION #4

  `SELECT count(*) FROM public.green_tripdata where filename like 'green_tripdata_2020%'`

> ANSWER: 1,734,051

--------------------

- QUESTION #5

added in kestra/postgres_taxi.yaml file in Kestra, 2021 year and ran 2021-03-yellow

*id: year*<br>
*type: SELECT*<br>
*displayName: Select year*<br>
*values: ["2019", "2020", "2021"]*<br>
*defaults: "2019"*


    SELECT count(*) FROM public.yellow_tripdata
    where filename = 'yellow_tripdata_2021-03.csv'


> ANSWER: 1,925,152

--------------------

- QUESTION #6

checked documentation

*triggers:*<br>
  *- id: daily*<br>
    *type: io.kestra.plugin.core.trigger.Schedule*<br>
    *cron: "@daily"*<br>
    *timezone: America/New_York*

> ANSWER: Add a timezone property set to America/New_York in the Schedule trigger configuration

--------------------


