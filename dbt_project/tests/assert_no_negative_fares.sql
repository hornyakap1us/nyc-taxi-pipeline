-- tests/assert_no_negative_fares.sql
-- ------------------------------------
-- Singular test: returns rows that fail the assertion.
-- A test passes when this query returns 0 rows.
-- dbt will surface any returned rows as test failures.

select
    trip_key,
    fare_amount,
    total_amount
from {{ ref('fct_trips') }}
where fare_amount  < 0
   or total_amount < 0
