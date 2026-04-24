-- models/staging/stg_yellow_trips.sql
-- ------------------------------------
-- Staging layer: rename columns, cast types, filter obvious garbage.
-- NO joins or business logic here — that belongs in intermediate models.
-- Materialised as a view so it costs nothing to maintain.

with source as (

    select * from {{ source('raw', 'yellow_trips') }}

),

cleaned as (

    select
        -- Identifiers
        vendor_id,

        -- Timestamps
        cast(pickup_datetime  as timestamp) as pickup_at,
        cast(dropoff_datetime as timestamp) as dropoff_at,

        -- Derived time fields (useful for downstream aggregations)
        date(pickup_datetime)             as pickup_date,
        extract(hour  from pickup_datetime) as pickup_hour,
        extract(dayofweek from pickup_datetime) as pickup_day_of_week,  -- 1=Sun, 7=Sat

        -- Trip details
        cast(passenger_count as integer)  as passenger_count,
        cast(trip_distance    as numeric) as trip_distance_miles,

        -- Payment
        payment_type,
        cast(fare_amount   as numeric) as fare_amount,
        cast(tip_amount    as numeric) as tip_amount,
        cast(tolls_amount  as numeric) as tolls_amount,
        cast(total_amount  as numeric) as total_amount,

        -- Derived: trip duration in minutes
        timestamp_diff(
            cast(dropoff_datetime as timestamp),
            cast(pickup_datetime  as timestamp),
            minute
        ) as trip_duration_minutes

    from source

    -- Filter out obvious data quality issues
    where pickup_datetime  is not null
      and dropoff_datetime is not null
      and trip_distance    > 0
      and total_amount     > 0
      and passenger_count  > 0

),

deduped as (

    select *,
        row_number() over (
            partition by vendor_id, pickup_at, dropoff_at, trip_distance_miles, total_amount
            order by pickup_at
        ) as row_num
    from cleaned


)

select * from deduped
where row_num = 1
