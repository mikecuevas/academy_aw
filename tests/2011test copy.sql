with total_sales as (
  select
    sum(net_sales_amount) as total_sales_2011
  from {{ ref('fct_vendas') }}
  where orderdate between '2011-01-01' and '2011-12-31'
)

select
  total_sales_2011
from total_sales