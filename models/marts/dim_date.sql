-- models/marts/dimensions/dim_date.sql

with
    date_range as (
        select
            dateadd(day, seq4(), '2005-01-01') as date
        from table(generator(rowcount => 3652)) -- 10 anos de datas
    ),

    date_attributes as (
        select
            date as date_id,
            date,
            year(date) as year,
            month(date) as month,
            day(date) as day,
            dayofweek(date) as day_of_week,
            dayname(date) as day_name,
            monthname(date) as month_name,
            quarter(date) as quarter,
            weekofyear(date) as week_of_year,
            case when dayofweek(date) in (6, 7) then true else false end as is_weekend
        from date_range
    )

select * from date_attributes
