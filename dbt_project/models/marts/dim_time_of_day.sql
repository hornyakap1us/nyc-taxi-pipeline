-- models/marts/dim_time_of_day.sql
-- ----------------------------------
-- Dimension table: lookup for time-of-day categories.
-- Small static tables like this are a good intro to dbt seeds
-- or just inlined as a CTE (done here for simplicity).

{{ config(materialized='table') }}

with time_of_day as (

    select 'morning_rush' as time_of_day, 1 as sort_order, '6am – 9am'   as description
    union all
    select 'midday',                       2,               '10am – 3pm'
    union all
    select 'evening_rush',                 3,               '4pm – 7pm'
    union all
    select 'evening',                      4,               '8pm – 11pm'
    union all
    select 'overnight',                    5,               '12am – 5am'

)

select * from time_of_day
