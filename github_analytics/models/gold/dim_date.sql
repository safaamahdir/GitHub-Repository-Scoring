{{ config(materialized='table') }}

with date_spine as (
    select
        unnest(generate_series(
        current_date- interval '5 years',
        current_date,
        interval '1 day'
    ))::date as full_date
),

enriched as (
    select
        -- date_id: surrogatekeyinYYYYMMDDformat(integer).
        to_char(full_date, 'YYYYMMDD')::integer as date_id,

        -- full_date: thecompletedate.
        full_date,

        -- year,month,week_of_year,day_of_week: temporalextractions.
        extract(year from full_date) as year,
        extract(month from full_date) as month,
        extract(week from full_date) as week_of_year,
        extract(day from full_date) as day_of_week,

        --day_name,month_name: readablenames(Monday,January).
        to_char(full_date, 'Day') as day_name,
        to_char(full_date, 'Month') as month_name,

        --is_weekend: boolean(dow=0or6).
        case when extract(dow from full_date) in (0,6) then true else false end as is_weekend,

        -- quarter: quarter(1to4).
        extract(quarter from full_date) as quarter,

    
    from date_spine
)

select * from enriched