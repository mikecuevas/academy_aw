with
    sales_items as (
        select * from {{ ref('int') }}
    )
    , metrics as (
        select

            sk_sales
            ,sales_order_id
            ,sales_order_detail_id
            ,sales_reason_id
            ,pk_customerid
            ,salesperson_id
            ,bill_to_address_id
            ,ship_to_address_id
            ,pk_creditcardid
            ,product_id
            ,orderdate
            ,duedate
            ,shipdate

            cast(orderqty as numeric) as order_quantity
            ,cast(unitprice / count(sales_order_id || '-' || product_id) over(partition by sales_order_id || '-' || product_id) as numeric(18,4)) as unit_price_allocated
            ,cast(unitpricediscount / count(sales_order_id || '-' || product_id) over(partition by sales_order_id || '-' || product_id) as numeric(18,4)) as unit_price_discount_allocated
            ,cast(orderqty as numeric) * unit_price_allocated as extended_amount
            ,(cast(orderqty as numeric) * unit_price_allocated) * unit_price_discount_allocated as discount_amount
            ,(cast(orderqty as numeric) * unit_price_allocated) * (1 - unit_price_discount_allocated) as net_sales_amount
            ,cast(subtotal / count(sales_order_id) over(partition by sales_order_id) as numeric(18,4)) as subtotal_allocated,
            ,cast(taxamt / count(sales_order_id) over(partition by sales_order_id) as numeric(18,4)) as tax_amount_allocated,
            ,cast(freight / count(sales_order_id) over(partition by sales_order_id) as numeric(18,4)) as freight_allocated,
            ,cast(totaldue / count(sales_order_id) over(partition by sales_order_id) as numeric(18,4)) as total_due_allocated

        from sales_items
    )
select
*
from metrics
