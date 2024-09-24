-- models/marts/facts/fact_sales.sql

with
    sales_items as (
        select * from {{ ref('int') }}
    )
, metrics as (
    select
        -- Chaves
        sk_sales,
        sales_order_id,
        sales_order_detail_id,
        sales_reason_id,
        customer_id,
        salesperson_id,
        bill_to_address_id,
        ship_to_address_id,
        creditcard_id,
        product_id,

        -- Datas
        orderdate,
        duedate,
        shipdate,

        -- Medidas de quantidade e preço
        orderqty as order_quantity,
        unitprice as unit_price,
        unitpricediscount as unit_price_discount,

        -- Cálculos financeiros
        order_quantity * unit_price as extended_amount,
        (order_quantity * unit_price) * unit_price_discount as discount_amount,
        (order_quantity * unit_price) - ((order_quantity * unit_price) * unit_price_discount) as net_sales_amount,

        -- Rateio dos valores agregados
        subtotal / count(sales_order_id) over(partition by sales_order_id) as subtotal_allocated,
        taxamt / count(sales_order_id) over(partition by sales_order_id) as tax_amount_allocated,
        freight / count(sales_order_id) over(partition by sales_order_id) as freight_allocated,
        totaldue / count(sales_order_id) over(partition by sales_order_id) as total_due_allocated

    from sales_items
)
select
    -- Chaves
    sk_sales,
    sales_order_id,
    sales_order_detail_id,
    sales_reason_id,
    customer_id,
    salesperson_id,
    bill_to_address_id,
    ship_to_address_id,
    creditcard_id,
    product_id,

    -- Datas
    orderdate,
    duedate,
    shipdate,

    -- Medidas
    order_quantity,
    unit_price,
    unit_price_discount,
    extended_amount,
    discount_amount,
    net_sales_amount,
    subtotal_allocated,
    tax_amount_allocated,
    freight_allocated,
    total_due_allocated

from metrics
