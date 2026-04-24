-- models/marts/dim_pickup_zones.sql
-- -----------------------------------
-- Dimension table: NYC taxi zone lookup.
-- Joins to fct_trips on pickup_location_id and dropoff_location_id.
-- Source: bigquery-public-data.new_york_taxi_trips.taxi_zone_geom

{{ config(materialized='table') }}

with source as (

    select
        zone_id,
        zone_name,
        borough
    from `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom`

),

deduped as (

    select *,
        row_number() over (
            partition by zone_id
            order by zone_id
        ) as row_num
    from source

),

final as (

    select
        zone_id,
        zone_name,
        borough,
        case borough
            when 'Manhattan'     then 'core'
            when 'Brooklyn'      then 'outer'
            when 'Queens'        then 'outer'
            when 'Bronx'         then 'outer'
            when 'Staten Island' then 'outer'
            else 'unknown'
        end as borough_region
    from deduped
    where row_num = 1

)

select * from final