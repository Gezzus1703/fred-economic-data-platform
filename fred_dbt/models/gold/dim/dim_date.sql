with date_bounds as (

    select
        min(observation_date) as min_date,
        max(observation_date) as max_date

    from {{ ref("int_fred_observations_latest") }}

),

day_numbers as (

    select
        row_number() over (order by seq4()) - 1 as day_number

    from table(generator(rowcount => 100000))

),

date_spine as (

    select
        dateadd(day, day_number, min_date) as date_day

    from day_numbers
    cross join date_bounds

    where dateadd(day, day_number, min_date) <= max_date

)

select
    to_number(to_char(date_day, 'YYYYMMDD')) as date_key,
    date_day,
    year(date_day) as year_number,
    quarter(date_day) as quarter_number,
    month(date_day) as month_number,
    monthname(date_day) as month_name,
    to_char(date_day, 'YYYY-MM') as year_month,
    day(date_day) as day_of_month,
    dayname(date_day) as day_name,
    weekiso(date_day) as iso_week_number,
    dayofweekiso(date_day) in (6, 7) as is_weekend

from date_spine