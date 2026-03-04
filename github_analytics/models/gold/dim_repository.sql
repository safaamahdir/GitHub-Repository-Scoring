{{ config(materialized='table') }}

with date_spine as (
    select
        unnest(generate_series(
            current_date - interval '5 years',
            current_date,
            interval '1 day'
        ))::date as full_date
),
enriched as (
    select
        -- Surrogate key: YYYYMMDD as integer
        cast(strftime(full_date, '%Y%m%d') as integer)   as date_id,
        full_date,

        -- Temporal extractions
        extract(year from full_date)::integer             as year,
        extract(month from full_date)::integer            as month,
        extract(week from full_date)::integer             as week_of_year,
        extract(dow from full_date)::integer              as day_of_week,

        -- Readable names
        strftime(full_date, '%A')                         as day_name,
        strftime(full_date, '%B')                         as month_name,

        -- Weekend flag (Sunday=0, Saturday=6)
        extract(dow from full_date) in (0, 6)             as is_weekend,

        -- Quarter
        extract(quarter from full_date)::integer          as quarter

    from date_spine
)
select * from enriched