with 

source as (

    select * from {{ source('adventurework_erp', 'SalesReason') }}

),

salesreason as (

    select
        cast(salesreasonid as int) as pk_salesreasonid
        ,cast(name as varchar) as salesreason_name
        ,cast(reasontype as varchar) as reasontype

    from source

)

select * from salesreason
