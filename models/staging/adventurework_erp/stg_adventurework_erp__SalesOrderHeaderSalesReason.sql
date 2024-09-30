with source as (

    select * from {{ source('adventurework_erp', 'SalesOrderHeaderSalesReason') }}

),

salesorderheadersalesreason as (

    select
        cast(salesorderid as int) as fk_salesorderid
      , cast(salesreasonid as int) as fk_salesreasonid

    from source

)

select * from salesorderheadersalesreason
