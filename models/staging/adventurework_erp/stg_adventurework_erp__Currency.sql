with 

source as (

    select * from {{ source('adventurework_erp', 'Currency') }}

),

currency as (

    select
        currencycode,
        name,
        modifieddate

    from source

)

select * from currency
