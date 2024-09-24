with 

source as (

    select * from {{ source('adventurework_erp', 'Currency') }}

),

currency as (

    select
        cast(currencycode as varchar) as pk_currencycode
        ,cast(name as varchar) as currency_name
        ,cast(modifieddate as date) as modifieddate

    from source

)

select * from currency
