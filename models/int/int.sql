-- models/intermediate/int_sales_items.sql

with
    sales_order_header as (
        select * from {{ ref('stg_adventurework_erp__SalesOrderHeader') }}
    ),

    sales_order_detail as (
        select * from {{ ref('stg_adventurework_erp__SalesOrderDetail') }}
    ),

    sales_order_reason as (
        select * from {{ ref('stg_adventurework_erp__SalesOrderHeaderSalesReason') }}
    ),

    joined as (
        select
            -- Chaves
            soh.pk_salesorderid as sales_order_id,
            sod.pk_salesorderdetailid as sales_order_detail_id,
            soh.fk_customerid as customer_id,
            soh.fk_salespersonid as salesperson_id,
            soh.fk_billtoaddressid as bill_to_address_id,
            soh.fk_shiptoaddressid as ship_to_address_id,
            soh.fk_creditcardid as creditcard_id,
            sod.fk_productid as product_id,
            sor.fk_salesreasonid as sales_reason_id,

            -- Datas
            soh.orderdate,
            soh.duedate,
            soh.shipdate,

            -- Outros campos
            sod.orderqty,
            sod.unitprice,
            sod.unitpricediscount,
            soh.subtotal,
            soh.taxamt,
            soh.freight,
            soh.totaldue

        from sales_order_header soh
        left join sales_order_detail sod on soh.pk_salesorderid = sod.pk_salesorderid
        left join sales_order_reason sor on soh.pk_salesorderid = sor.fk_salesorderid
    ),

    created_surrogate_key as (
        select
            -- Criando a surrogate key concatenando as chaves naturais
            md5(cast(sales_order_id as varchar) || '-' || cast(sales_order_detail_id as varchar) || '-' || coalesce(cast(sales_reason_id as varchar), '')) as sk_sales,

            -- Incluindo todas as colunas
            *
        from joined
    )

select *
from created_surrogate_key
