with source as (
      select * from {{ source('adventurework_erp', 'SalesOrderHeaderSalesReason') }}
),
SalesOrderHeaderSalesReason as (
    select
        cast(SALESORDERID as int) as FK_SALESORDERID
        ,cast(SALESREASONID as int) as FK_SALESREASONID
        ,cast(MODIFIEDDATE as date) as MODIFIEDDATE

    from source
)
select * from SalesOrderHeaderSalesReason
  