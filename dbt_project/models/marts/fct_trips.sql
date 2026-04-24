-- models/marts/fct_trips.sql
-- --------------------------
-- Fact table: one row per trip, queryable by analysts and BI tools.
-- Uses an INCREMENTAL materialization — on the first run it builds the full
-- table; on subsequent runs it only processes new data (much cheaper).
--
-- Run full refresh: dbt run --full-refresh --select fct_trips
-- Run incremental:  dbt run --select fct_trips

{{
    config(
        materialized = 'incremental',
        unique_key   = 'trip_key',
        partition_by = {
            "field": "pickup_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by   = ['time_of_day', 'payment_method']
    )
}}

with enriched as (

    select * from {{ ref('int_trips_enriched') }}

    -- On incremental runs, only process new partitions
    {% if is_incremental() %}
        where pickup_date > (select max(pickup_date) from {{ this }})
    {% endif %}

),

final as (

    select
        -- Surrogate key: hash of the natural key fields
        {{ dbt_utils_generate_surrogate_key(['vendor_id', 'pickup_at', 'dropoff_at', 'trip_distance_miles', 'total_amount']) }} as trip_key,

        -- Dimensions
        vendor_id,
        pickup_date,
        pickup_hour,
        time_of_day,
        day_type,
        trip_distance_category,
        payment_method,

        -- Timestamps
        pickup_at,
        dropoff_at,

        -- Measures
        passenger_count,
        trip_distance_miles,
        trip_duration_minutes,
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,
        tip_pct,

        -- Data quality flag
        is_outlier,

        -- Metadata
        current_timestamp() as dbt_updated_at

    from enriched

)

select * from final
