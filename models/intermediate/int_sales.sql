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
            soh.pk_salesorderid
          , sod.pk_salesorderdetailid
          , soh.fk_customerid
          , soh.fk_salespersonid
          , soh.fk_creditcardid
          , sod.fk_productid
          , sor.fk_salesreasonid
          , soh.orderdate
          , soh.duedate
          , soh.shipdate
          , sod.orderqty
          , sod.unitprice
          , sod.unitpricediscount
          , soh.subtotal
          , soh.taxamt
          , soh.freight
          , soh.totaldue
        from sales_order_header soh
        left join sales_order_detail sod on soh.pk_salesorderid = sod.fk_salesorderid
        left join sales_order_reason sor on soh.pk_salesorderid = sor.fk_salesorderid
    ),

    sales_with_sk as (
        select
            {{ dbt_utils.generate_surrogate_key([
                'pk_salesorderid',
                'pk_salesorderdetailid',
                'coalesce(fk_salesreasonid, 0)'
            ]) }} as sales_sk
          , cust.customer_sk
          , sp.salesperson_sk
          , cc.creditcard_sk
          , prod.product_sk
          , sr.salesreason_sk
          , joined.orderdate
          , joined.duedate
          , joined.shipdate
          , joined.orderqty
          , joined.unitprice
          , joined.unitpricediscount
          , joined.subtotal
          , joined.taxamt
          , joined.freight
          , joined.totaldue
        from joined
        left join {{ ref('dim_clients') }} cust on joined.fk_customerid = cust.pk_customerid
        left join {{ ref('dim_salesperson') }} sp on joined.fk_salespersonid = sp.pk_businessentityid
        left join {{ ref('dim_creditcard') }} cc on joined.fk_creditcardid = cc.pk_creditcardid
        left join {{ ref('dim_product') }} prod on joined.fk_productid = prod.pk_productid
        left join {{ ref('dim_salesreason') }} sr on joined.fk_salesreasonid = sr.pk_salesreasonid
    )

select
    sales_sk
  , customer_sk
  , salesperson_sk
  , creditcard_sk
  , product_sk
  , salesreason_sk
  , orderdate
  , duedate
  , shipdate
  , orderqty
  , unitprice
  , unitpricediscount
  , subtotal
  , taxamt
  , freight
  , totaldue
from sales_with_sk
