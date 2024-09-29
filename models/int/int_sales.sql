with
    sales_order_header as (
        select * from {{ ref('stg_adventurework_erp__SalesOrderHeader') }}
    )

    , sales_order_detail as (
        select * from {{ ref('stg_adventurework_erp__SalesOrderDetail') }}
    )

    , sales_order_reason as (
        select * from {{ ref('stg_adventurework_erp__SalesOrderHeaderSalesReason') }}
    )

    , joined as (
        select
            
            soh.pk_salesorderid
            ,sod.fk_salesorderdetailid
            ,soh.fk_customerid
            ,soh.fk_salespersonid
            ,soh.fk_billtoaddressid
            ,soh.fk_shiptoaddressid
            ,soh.fk_creditcardid
            ,sod.fk_productid
            ,sor.fk_salesreasonid
            ,soh.orderdate
            ,soh.duedate
            ,soh.shipdate
            ,sod.orderqty
            ,sod.unitprice
            ,sod.unitpricediscount
            ,soh.subtotal
            ,soh.taxamt
            ,soh.freight
            ,soh.totaldue

        from sales_order_header soh
        left join sales_order_detail sod on soh.pk_salesorderid = sod.pk_salesorderid
        left join sales_order_reason sor on soh.pk_salesorderid = sor.fk_salesorderid
    ),

    created_surrogate_key as (
        select
            md5(cast(sales_order_header as varchar) || '-' || cast(sales_order_detail as varchar) || '-' || coalesce(cast(sales_order_reason as varchar), '')) as sk_sales,
            *
        from joined
    )

select *
from created_surrogate_key
