with source as (
      select * from {{ source('adventurework_erp', 'SalesOrderHeaderSalesReason') }}
),
SalesOrderHeaderSalesReason as (
    select
        cast(salesorderid as int) as fk_salesorderid
        ,cast(salesreasonid as int) as fk_salesreasonid

    from source
)
select * from SalesOrderHeaderSalesReason
  