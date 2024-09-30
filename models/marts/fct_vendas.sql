with
    sales_data as (
        select * from {{ ref('int_sales') }}
    ),

    metrics as (
        select
            sales_sk,
            customer_sk,
            salesperson_sk,
            creditcard_sk,
            product_sk,
            salesreason_sk,
            orderdate,
            duedate,
            shipdate,
            cast(orderqty as numeric) as order_quantity,
            cast(unitprice / count(*) over(partition by sales_sk, product_sk) as numeric(18,4)) as unit_price_allocated,
            cast(unitpricediscount / count(*) over(partition by sales_sk, product_sk) as numeric(18,4)) as unit_price_discount_allocated,
            cast(orderqty as numeric) * unit_price_allocated as extended_amount,
            (cast(orderqty as numeric) * unit_price_allocated) * unit_price_discount_allocated as discount_amount,
            (cast(orderqty as numeric) * unit_price_allocated) * (1 - unit_price_discount_allocated) as net_sales_amount,
            cast(subtotal / count(*) over(partition by sales_sk) as numeric(18,4)) as subtotal_allocated,
            cast(taxamt / count(*) over(partition by sales_sk) as numeric(18,4)) as tax_amount_allocated,
            cast(freight / count(*) over(partition by sales_sk) as numeric(18,4)) as freight_allocated,
            cast(totaldue / count(*) over(partition by sales_sk) as numeric(18,4)) as total_due_allocated
        from sales_data
    )

select
    sales_sk,
    customer_sk,
    salesperson_sk,
    creditcard_sk,
    product_sk,
    salesreason_sk,
    orderdate,
    duedate,
    shipdate,
    order_quantity,
    unit_price_allocated,
    unit_price_discount_allocated,
    extended_amount,
    discount_amount,
    net_sales_amount,
    subtotal_allocated,
    tax_amount_allocated,
    freight_allocated,
    total_due_allocated
from metrics
