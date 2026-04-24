-- models/intermediate/int_trips_enriched.sql
-- -------------------------------------------
-- Intermediate layer: enrich trips with derived business logic.
-- This is where you'd join in dimension tables (e.g. borough lookups),
-- apply business rules, and flag edge cases.
-- Still materialised as a view — no storage cost.

with trips as (

    select * from {{ ref('stg_yellow_trips') }}

),

enriched as (

    select
        -- Pass through all staging columns
        trips.*,

        -- Classify time of day (useful for analysis)
        case
            when pickup_hour between 6  and 9  then 'morning_rush'
            when pickup_hour between 10 and 15 then 'midday'
            when pickup_hour between 16 and 19 then 'evening_rush'
            when pickup_hour between 20 and 23 then 'evening'
            else 'overnight'
        end as time_of_day,

        -- Classify day type
        case
            when pickup_day_of_week in (1, 7) then 'weekend'
            else 'weekday'
        end as day_type,

        -- Classify trip distance
        case
            when trip_distance_miles < 1   then 'short'
            when trip_distance_miles < 5   then 'medium'
            when trip_distance_miles < 15  then 'long'
            else 'very_long'
        end as trip_distance_category,

        -- Payment label (human-readable)
        case payment_type
            when '1' then 'credit_card'
            when '2' then 'cash'
            when '3' then 'no_charge'
            when '4' then 'dispute'
            else 'other'
        end as payment_method,

        -- Tip percentage (only meaningful for credit card trips)
        case
            when payment_type = '1' and fare_amount > 0
                then round(tip_amount / fare_amount * 100, 2)
            else null
        end as tip_pct,

        -- Flag trips that look like data quality issues
        case
            when trip_duration_minutes > 300 then true
            when trip_distance_miles   > 100 then true
            when total_amount          > 500 then true
            else false
        end as is_outlier

    from trips

)

select * from enriched
